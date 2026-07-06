using System.Net;
using System.Text;
using System.Text.Json;
using Azure.Storage.Blobs; 
using ClosedXML.Excel;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;

namespace NorrinAcademy.Functions;

public class DataLakeAdapter
{
    private readonly ILogger<DataLakeAdapter> _logger;

    public DataLakeAdapter(ILogger<DataLakeAdapter> logger)
    {
        _logger = logger;
    }

    [Function("DataLakeAdapter")]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Function, "post", Route = "datalakeadapter")] HttpRequestData req)
    {
        var requestBody = await new StreamReader(req.Body).ReadToEndAsync();
        _logger.LogInformation("DataLakeAdapter received request.");

        if (!string.IsNullOrWhiteSpace(requestBody))
        {
            using var doc = JsonDocument.Parse(requestBody);
            var root = doc.RootElement;

            if (root.ValueKind == JsonValueKind.Array && root.GetArrayLength() > 0)
            {
                var firstEvent = root[0];
                var eventType = firstEvent.TryGetProperty("eventType", out var eventTypeProperty) ? eventTypeProperty.GetString() : null;
                var eventId = firstEvent.TryGetProperty("id", out var idProperty) ? idProperty.GetString() : null;
                var eventSubject = firstEvent.TryGetProperty("subject", out var subjectProperty) ? subjectProperty.GetString() : null;

                _logger.LogInformation("EventGrid payload: type={EventType}, id={EventId}, subject={EventSubject}", eventType, eventId, eventSubject);

                if (TryGetSubscriptionValidationResponse(firstEvent, out var validationCode))
                {
                    var validationResponse = req.CreateResponse(HttpStatusCode.OK);
                    validationResponse.Headers.Add("Content-Type", "application/json");
                    await validationResponse.WriteStringAsync($"{{\n  \"validationResponse\": \"{validationCode}\"\n}}");
                    return validationResponse;
                }

                if (TryGetBlobUrl(firstEvent, out var blobUrl))
                {
                    BlobClient sourceBlobClient;
                    BlobServiceClient blobServiceClient;

                    try
                    {
                        var connectionString = Environment.GetEnvironmentVariable("CUSTOMCONNSTR_AzureStorageDataPull") 
                                            ?? Environment.GetEnvironmentVariable("AzureStorageDataPull");

                        if (string.IsNullOrEmpty(connectionString))
                        {
                            throw new InvalidOperationException("Missing variable AzureStorageDataPull or CUSTOMCONNSTR_AzureStorageDataPull in Function App settings.");
                        }

                        blobServiceClient = new BlobServiceClient(connectionString);
                        
                        var path = blobUrl.TrimStart('/');
                        var slashIndex = path.IndexOf('/');
                        if (slashIndex > 0)
                        {
                            sourceBlobClient = blobServiceClient.GetBlobContainerClient(path.Substring(0, slashIndex)).GetBlobClient(path.Substring(slashIndex + 1));
                        }
                        else
                        {
                            sourceBlobClient = blobServiceClient.GetBlobContainerClient("pre-raw").GetBlobClient(path);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error while creating BlobClient: {BlobUrl}", blobUrl);
                        return await CreatePlainTextResponse(req, HttpStatusCode.BadRequest, $"Invalid config/url: {ex.Message}");
                    }

                    var fileName = Path.GetFileName(sourceBlobClient.Name);
                    var extension = Path.GetExtension(fileName).ToLowerInvariant();

                    _logger.LogInformation("DataLakeAdapter processing blob {BlobUrl} (file={FileName}).", blobUrl, fileName);

                    // Download into RAM
                    var sourceStream = new MemoryStream();
                    try
                    {
                        await sourceBlobClient.DownloadToAsync(sourceStream);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Failed to download blob from {BlobUrl}", blobUrl);
                        return await CreatePlainTextResponse(req, HttpStatusCode.OK, "failed to download source blob");
                    }

                    sourceStream.Position = 0;

                    string csvContent;
                    try
                    {
                        csvContent = extension == ".csv"
                            ? await ReadStreamAsTextAsync(sourceStream)
                            : ConvertExcelToCsv(sourceStream);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to convert file {FileName} to CSV.", fileName);
                        return await CreatePlainTextResponse(req, HttpStatusCode.OK, "conversion failure");
                    }

                    var csvFileName = Path.GetFileNameWithoutExtension(fileName) + ".csv";

                    try
                    {
                        await WriteToStagingAsync(csvContent, csvFileName, blobServiceClient);
                        await ArchiveOriginalFileAsync(sourceStream, fileName, blobServiceClient);
                        
                        // Cleaning pre-raw after successful proocessing
                        await sourceBlobClient.DeleteIfExistsAsync();
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to write staging/archive file for {FileName}.", fileName);
                        return await CreatePlainTextResponse(req, HttpStatusCode.InternalServerError, $"Processing failure: {ex.Message}");
                    }

                    _logger.LogInformation("Hoàn tất: Đã convert {FileName} → {CsvFileName}, đẩy vào staging, backup ở archive và xóa file cũ.", fileName, csvFileName);
                }
            }
        }

        return await CreatePlainTextResponse(req, HttpStatusCode.OK, "accepted");
    }

    private static bool TryGetSubscriptionValidationResponse(JsonElement eventElement, out string? validationCode)
    {
        validationCode = null;
        if (!eventElement.TryGetProperty("eventType", out var eventType) || eventType.GetString() != "Microsoft.EventGrid.SubscriptionValidationEvent") return false;
        if (!eventElement.TryGetProperty("data", out var data) || data.ValueKind != JsonValueKind.Object || !data.TryGetProperty("validationCode", out var code)) return false;
        validationCode = code.GetString();
        return !string.IsNullOrWhiteSpace(validationCode);
    }

    private static bool TryGetBlobUrl(JsonElement eventElement, out string blobUrl)
    {
        blobUrl = string.Empty;
        if (!eventElement.TryGetProperty("data", out var data) || data.ValueKind != JsonValueKind.Object) return false;
        if (!data.TryGetProperty("blobUrl", out var urlProp)) return false;
        blobUrl = urlProp.GetString() ?? string.Empty;
        return !string.IsNullOrWhiteSpace(blobUrl);
    }

    private static async Task<string> ReadStreamAsTextAsync(Stream stream)
    {
        stream.Position = 0;
        using var reader = new StreamReader(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, leaveOpen: true);
        return await reader.ReadToEndAsync();
    }

    private static string ConvertExcelToCsv(Stream excelStream)
    {
        excelStream.Position = 0;
        using var workbook = new XLWorkbook(excelStream);
        var worksheet = workbook.Worksheets.First();
        var range = worksheet.RangeUsed();
        if (range is null) return string.Empty;

        var builder = new StringBuilder();
        foreach (var row in range.RowsUsed())
        {
            var values = row.Cells().Select(c => EscapeCsvField(c.GetValue<string>()));
            builder.AppendLine(string.Join(",", values));
        }
        return builder.ToString();
    }

    private static string EscapeCsvField(string? field)
    {
        if (string.IsNullOrEmpty(field)) return string.Empty;
        var escaped = field.Replace("\"", "\"\"");
        if (escaped.Contains(',') || escaped.Contains('"') || escaped.Contains('\n') || escaped.Contains('\r')) return $"\"{escaped}\"";
        return escaped;
    }

    private static async Task<HttpResponseData> CreatePlainTextResponse(HttpRequestData req, HttpStatusCode statusCode, string content)
    {
        var response = req.CreateResponse(statusCode);
        response.Headers.Add("Content-Type", "text/plain");
        await response.WriteStringAsync(content);
        return response;
    }

    private static async Task WriteToStagingAsync(string csvContent, string csvFileName, BlobServiceClient blobServiceClient)
    {
        var stagingContainer = blobServiceClient.GetBlobContainerClient("staging");
        await stagingContainer.CreateIfNotExistsAsync();

        var destBlob = stagingContainer.GetBlobClient(csvFileName);
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));
        await destBlob.UploadAsync(stream, overwrite: true);
    }
    private static async Task ArchiveOriginalFileAsync(Stream sourceStream, string fileName, BlobServiceClient blobServiceClient)
    {
        var archiveContainer = blobServiceClient.GetBlobContainerClient("archive");
        await archiveContainer.CreateIfNotExistsAsync();

        var destBlob = archiveContainer.GetBlobClient(fileName);
        sourceStream.Position = 0;
        await destBlob.UploadAsync(sourceStream, overwrite: true);
    }
}