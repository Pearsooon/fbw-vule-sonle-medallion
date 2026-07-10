using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.Data.SqlClient;
using SendGrid;
using SendGrid.Helpers.Mail;

namespace Company.Function;

public class Consumer
{
    private readonly ILogger<Consumer> _logger;
    private readonly string? _lakehouseConnectionString;
    private readonly string? _tenantId;

    public Consumer(ILogger<Consumer> logger)
    {
        _logger = logger;
        // Read directly from App settings
        _lakehouseConnectionString = Environment.GetEnvironmentVariable("FabricLakehouseConnection");
        _tenantId = Environment.GetEnvironmentVariable("AzureTenantId");
    }

    [Function(nameof(Consumer))]
    public async Task Run(
        [ServiceBusTrigger("nrn-academy-sbq-query-son", Connection = "ServiceBusConnection")]
        ServiceBusReceivedMessage message,
        ServiceBusMessageActions messageActions)
    {
        _logger.LogInformation("[Consumer] Started processing Message ID: {id}", message.MessageId);

        try
        {
            var bodyString = message.Body.ToString();

            if (string.IsNullOrWhiteSpace(bodyString))
            {
                throw new Exception("Message body is empty.");
            }

            // Parse JSON from message
            using var doc = JsonDocument.Parse(bodyString);
            var root = doc.RootElement;

            // Extract info from payload (supports both nested {"body":{...}} or flat JSON)
            string? sqlQuery = null;
            string? recipientEmail = null;
            string? requestId = null;

            var dataElement = root.TryGetProperty("body", out var bodyEl) ? bodyEl : root;

            if (dataElement.TryGetProperty("query", out var queryEl))
                sqlQuery = queryEl.GetString();
            if (dataElement.TryGetProperty("recipientEmail", out var emailEl))
                recipientEmail = emailEl.GetString();
            if (dataElement.TryGetProperty("requestId", out var reqIdEl))
                requestId = reqIdEl.GetString();

            if (string.IsNullOrWhiteSpace(sqlQuery))
            {
                throw new Exception("Field 'query' not found in payload. Expected format: { \"query\": \"SELECT ...\", \"recipientEmail\": \"...\" }");
            }

            _logger.LogInformation("[Consumer] Executing query for requestId={requestId}: {query}", requestId, sqlQuery);

            // Check Lakehouse configuration
            if (string.IsNullOrWhiteSpace(_lakehouseConnectionString))
            {
                throw new Exception("Missing environment variable 'FabricLakehouseConnection'. Please add it to App Settings.");
            }

            // Query Gold layer from Fabric Lakehouse SQL Endpoint
            var (columns, resultRows) = await QueryGoldLayerAsync(sqlQuery);
            _logger.LogInformation("[Consumer] Query returned {rowCount} rows", resultRows.Count);

            // Create result payload
            var resultPayload = new
            {
                requestId = requestId,
                query = sqlQuery,
                status = "processed",
                timestamp = DateTime.UtcNow,
                rowCount = resultRows.Count,
                data = resultRows
            };

            // Send results via Email (SendGrid)
            await SendEmailViaSendGridAsync(recipientEmail, resultPayload, requestId);
            _logger.LogInformation("[Consumer] Email sent successfully.");

            // Complete the message
            await messageActions.CompleteMessageAsync(message);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[Consumer] ERROR at Message ID {id}: {msg}", message.MessageId, ex.Message);

            try
            {
                await messageActions.DeadLetterMessageAsync(
                    message,
                    deadLetterReason: "ProcessingFailed",
                    deadLetterErrorDescription: ex.Message);
            }
            catch (Exception dlEx)
            {
                _logger.LogError(dlEx, "[Consumer] Error while pushing to Dead-letter: {msg}", dlEx.Message);
            }
        }
    }

    /// <summary>
    /// Executes the user-provided SQL query on the Gold layer of Fabric Lakehouse (via SQL Endpoint).
    /// Uses FabricLakehouseConnection.
    /// </summary>
    private async Task<(List<string> Columns, List<Dictionary<string, object?>> Rows)> QueryGoldLayerAsync(string sqlQuery)
    {
        var columns = new List<string>();
        var rows = new List<Dictionary<string, object?>>();

        try
        {
            var builder = new SqlConnectionStringBuilder(_lakehouseConnectionString);
            
            // Remove old auth info from connection string to manually fetch token
            string userId = builder.UserID;
            string password = builder.Password;
            
            builder.Remove("Authentication");
            builder.Remove("User ID");
            builder.Remove("Password");
            
            // Force longer timeout because Fabric SQL Endpoint might be sleeping and takes 30-40s to wake up
            builder.ConnectTimeout = 120;
            
            using var connection = new SqlConnection(builder.ConnectionString);

            if (!string.IsNullOrEmpty(userId) && !string.IsNullOrEmpty(password) && !string.IsNullOrEmpty(_tenantId))
            {
                var credential = new Azure.Identity.ClientSecretCredential(_tenantId, userId, password);
                var tokenResult = await credential.GetTokenAsync(new Azure.Core.TokenRequestContext(new[] { "https://database.windows.net/.default" }));
                connection.AccessToken = tokenResult.Token;
            }
            else
            {
                _logger.LogWarning("[Consumer] Missing AzureTenantId or connection string lacks User ID/Password. Will attempt connection using built-in Authentication.");
                connection.ConnectionString = _lakehouseConnectionString; // Fallback
            }

            await connection.OpenAsync();
            _logger.LogInformation("[Consumer] Successfully connected to Fabric Lakehouse.");

            using var cmd = connection.CreateCommand();
            cmd.CommandText = sqlQuery;
            cmd.CommandTimeout = 180; // 3-minute timeout for large queries

            using var reader = await cmd.ExecuteReaderAsync();

            // Collect column names
            for (int i = 0; i < reader.FieldCount; i++)
                columns.Add(reader.GetName(i));

            // Collect each row of results
            while (await reader.ReadAsync())
            {
                var row = new Dictionary<string, object?>();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    var value = reader.GetValue(i);
                    row[columns[i]] = value == DBNull.Value ? null : value;
                }
                rows.Add(row);
            }

            if (rows.Count == 0)
                _logger.LogWarning("[Consumer] Query returned no data.");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[Consumer] ERROR querying Gold layer: {msg}", ex.Message);
            throw;
        }

        return (columns, rows);
    }

    private async Task SendEmailViaSendGridAsync(string? recipientEmail, object resultPayload, string? requestId)
    {
        string? apiKey = Environment.GetEnvironmentVariable("SendGridApiKey");
        string? senderAddress = Environment.GetEnvironmentVariable("SenderEmail");
        string? recipient = recipientEmail;

        if (string.IsNullOrEmpty(apiKey))
        {
            _logger.LogError("[Consumer] Missing SendGridApiKey");
            return;
        }
        if (string.IsNullOrEmpty(senderAddress))
        {
            _logger.LogError("[Consumer] Missing SenderEmail");
            return;
        }
        if (string.IsNullOrEmpty(recipient))
        {
            _logger.LogError("[Consumer] Missing recipientEmail");
            return;
        }

        try
        {
            string resultJson = JsonSerializer.Serialize(resultPayload, new JsonSerializerOptions
            {
                WriteIndented = true
            });

            var subject = $"Query Results — nrn-academy Gold Layer{(requestId != null ? $" [{requestId}]" : "")}";

            // styling result for a more readable result
            var htmlContent = $@"<html>
<body style='font-family: Segoe UI, -apple-system, sans-serif; color: #333; line-height: 1.6; max-width: 900px; margin: 0 auto; padding: 24px;'>
    <h2 style='color: #0078d4; margin-bottom: 4px;'>Fabric Lakehouse Query Results</h2>
    <p style='color: #666; margin-top: 0;'>Gold layer · {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC{(requestId != null ? $" · Request: <code>{requestId}</code>" : "")}</p>
    <pre style='background: #f5f5f5; border-left: 4px solid #0078d4; padding: 16px; overflow-x: auto; font-size: 13px; border-radius: 4px;'>{System.Net.WebUtility.HtmlEncode(resultJson)}</pre>
    <hr style='border: none; border-top: 1px solid #ddd; margin: 20px 0;' />
    <p style='color: #999; font-size: 12px;'>Automated email from nrn-academy Azure Function Consumer. Please do not reply.</p>
</body>
</html>";

            var client = new SendGridClient(apiKey);
            var from = new EmailAddress(senderAddress, "nrn-academy System");
            var to = new EmailAddress(recipient);
            var msg = MailHelper.CreateSingleEmail(from, to, subject, resultJson, htmlContent);

            var response = await client.SendEmailAsync(msg);

            if (response.IsSuccessStatusCode)
            {
                _logger.LogInformation("[Consumer] Email sent successfully. StatusCode: {StatusCode}", response.StatusCode);
            }
            else
            {
                var body = await response.Body.ReadAsStringAsync();
                _logger.LogError("[Consumer] SendGrid returned an error. StatusCode: {StatusCode}, Body: {Body}", response.StatusCode, body);
                throw new Exception($"SendGrid error: {response.StatusCode} - {body}");
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[Consumer] ERROR sending email: {msg} | Type: {type}", ex.Message, ex.GetType().Name);
            if (ex.InnerException != null)
                _logger.LogError("[Consumer] InnerException: {msg}", ex.InnerException.Message);
            throw;
        }
    }
}