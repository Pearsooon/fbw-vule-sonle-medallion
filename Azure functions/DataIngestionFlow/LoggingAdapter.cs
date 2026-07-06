using System.Net;
using System.Text.Json;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;

namespace NorrinAcademy.Functions;

public class LoggingAdapter
{
    private readonly ILogger<LoggingAdapter> _logger;

    public LoggingAdapter(ILogger<LoggingAdapter> logger)
    {
        _logger = logger;
    }

    [Function("LoggingAdapter")]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "loggingadapter")] HttpRequestData req)
    {
        var requestBody = await new StreamReader(req.Body).ReadToEndAsync();

        if (!string.IsNullOrWhiteSpace(requestBody))
        {
            using var doc = JsonDocument.Parse(requestBody);
            var root = doc.RootElement;

            if (root.ValueKind == JsonValueKind.Array && root.GetArrayLength() > 0)
            {
                foreach (var eventElement in root.EnumerateArray())
                {
                    if (eventElement.TryGetProperty("eventType", out var eventTypeProp) && 
                        eventTypeProp.GetString() == "Microsoft.EventGrid.SubscriptionValidationEvent")
                    {
                        if (eventElement.TryGetProperty("data", out var data) &&
                            data.TryGetProperty("validationCode", out var code))
                        {
                            var validationResponse = req.CreateResponse(HttpStatusCode.OK);
                            validationResponse.Headers.Add("Content-Type", "application/json");
                            await validationResponse.WriteStringAsync($"{{\n  \"validationResponse\": \"{code.GetString()}\"\n}}");
                            return validationResponse;
                        }
                    }

                    var eventType = eventElement.TryGetProperty("eventType", out var et) ? et.GetString() : null;
                    var eventSubject = eventElement.TryGetProperty("subject", out var es) ? es.GetString() : null;
                    var eventData = eventElement.TryGetProperty("data", out var ed) ? ed.GetRawText() : null;

                    _logger.LogInformation("Audit trail: Ingestion event received. Event type: {EventType}, Event subject: {EventSubject}, Event data: {EventData}",
                        eventType, eventSubject, eventData);
                }
            }
        }

        var response = req.CreateResponse(HttpStatusCode.OK);
        response.Headers.Add("Content-Type", "text/plain");
        await response.WriteStringAsync("accepted");
        return response;
    }
}