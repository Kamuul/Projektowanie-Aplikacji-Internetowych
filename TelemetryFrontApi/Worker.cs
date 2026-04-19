using System;
using System.Collections.Generic; // DODANE dla Dictionary
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.SignalR;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Writes;

public class TelemetryWorker : BackgroundService
{
    private readonly IConnection _rabbitConnection;
    private readonly ILogger<TelemetryWorker> _logger;
    private readonly IHubContext<TelemetryHub> _hubContext;
    private IModel? _channel;
    private readonly Dictionary<string, DateTime> _lastAlertTimes = new Dictionary<string, DateTime>();
    private readonly TimeSpan _alertInterval = TimeSpan.FromSeconds(30);

    private const string InfluxUrl = "https://eu-central-1-1.aws.cloud2.influxdata.com";
    private const string InfluxToken = "FEVCx1wLyMwT1rc5yp79TTK5Zz60MLwcCEciennMPU8BXAS8704s98BqBUjXkTQ50CgAfIUIaQ7MMdjI9Z3zHA==";
    private const string InfluxOrg = "tak";
    private const string InfluxBucket = "system-telemetryczny";

    public TelemetryWorker(IConnection rabbitConnection, ILogger<TelemetryWorker> logger, IHubContext<TelemetryHub> hubContext)
    {
        _rabbitConnection = rabbitConnection;
        _logger = logger;
        _hubContext = hubContext;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Uruchamianie Workera nasłuchującego na dane z RabbitMQ...");

        _channel = _rabbitConnection.CreateModel();
        _channel.ExchangeDeclare("telemetry_exchange", "topic", true);

        string queueName = "telemetry_to_influx_queue";
        _channel.QueueDeclare(queueName, true, false, false, null);
        _channel.QueueBind(queueName, "telemetry_exchange", "telemetry.*", null);

        var consumer = new AsyncEventingBasicConsumer(_channel);
        consumer.Received += async (model, ea) =>
        {
            var body = ea.Body.ToArray();
            var messageString = Encoding.UTF8.GetString(body);

            try
            {
                await Task.Yield();
                await WriteToInfluxDbAsync(messageString);

                using var doc = JsonDocument.Parse(messageString);
                var deviceId = doc.RootElement.GetProperty("DeviceId").GetString() ?? "unknown";
                var decodedDataString = doc.RootElement.GetProperty("DecodedData").GetString();

                if (!string.IsNullOrWhiteSpace(decodedDataString))
                {
                    using var innerDoc = JsonDocument.Parse(decodedDataString);
                    if (innerDoc.RootElement.TryGetProperty("temperature", out var tempElement))
                    {
                        var temp = tempElement.GetDouble();
                        if (temp > 20.0)
                        {
                            bool canSendAlert = true;

                            if (_lastAlertTimes.TryGetValue(deviceId, out DateTime lastAlert))
                            {
                                if (DateTime.UtcNow - lastAlert < _alertInterval)
                                {
                                    canSendAlert = false; 
                                }
                            }

                            if (canSendAlert)
                            {
                                _lastAlertTimes[deviceId] = DateTime.UtcNow;

                                var alertMessage = $"[ALERT] Urzadzenie {deviceId} odnotowalo wysoką temperaturę: {temp} C!";
                                _logger.LogWarning(alertMessage);
                                await _hubContext.Clients.All.SendAsync("ReceiveAlert", alertMessage);
                            }
                        }
                    }
                }

                _channel.BasicAck(ea.DeliveryTag, false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[Worker] Błąd podczas przetwarzania wiadomości.");
                _channel.BasicNack(ea.DeliveryTag, false, false); 
            }
        };

        _channel.BasicConsume(queueName, false, "", false, false, null, consumer);
        return Task.CompletedTask;
    }

    private async Task WriteToInfluxDbAsync(string jsonMessage)
    {
        using var client = new InfluxDBClient(InfluxUrl, InfluxToken);
        var writeApi = client.GetWriteApiAsync();

        using var doc = JsonDocument.Parse(jsonMessage);
        var deviceId = doc.RootElement.GetProperty("DeviceId").GetString();

        var point = PointData
            .Measurement("sensor_telemetry")
            .Tag("device_id", deviceId)
            .Field("received_status", 1)
            .Timestamp(DateTime.UtcNow, WritePrecision.Ns);

        await writeApi.WritePointAsync(point, InfluxBucket, InfluxOrg);
        _logger.LogInformation("[Worker] Zapisano punkt pomiarowy do InfluxDB");
    }

    public override void Dispose()
    {
        _channel?.Dispose();
        base.Dispose();
    }
}
