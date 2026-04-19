using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.ObjectPool;
using RabbitMQ.Client;
using Microsoft.AspNetCore.SignalR;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddSignalR();

builder.Services.AddSingleton<IConnection>(sp =>
{
    var factory = new ConnectionFactory()
    {
        Uri = new Uri("amqps://gxwrqojv:9szEPXUDXCmRug8A9jmXs_iXHRRSDbaR@goose.rmq2.cloudamqp.com/gxwrqojv"),
        DispatchConsumersAsync = true
    };
    return factory.CreateConnection();
});

builder.Services.AddSingleton<IRabbitMqPublisher, RabbitMqPublisher>();
builder.Services.AddHostedService<TelemetryWorker>();

var app = builder.Build();

app.UseMiddleware<RequestLoggingMiddleware>();


app.UseDefaultFiles();
app.UseStaticFiles();

app.MapControllers();
app.MapHub<TelemetryHub>("/telemetryHub"); 


app.Run();




public class TelemetryHub : Hub
{
}



public class TelemetryRequest
{
    public string DeviceId { get; set; } = string.Empty;
    public DateTime Timestamp { get; set; }
    public string PayloadBase64 { get; set; } = string.Empty;
}



public class RequestLoggingMiddleware
{
    private readonly RequestDelegate _next;
    private readonly ILogger<RequestLoggingMiddleware> _logger;

    public RequestLoggingMiddleware(RequestDelegate next, ILogger<RequestLoggingMiddleware> logger)
    {
        _next = next;
        _logger = logger;
    }

    public async Task InvokeAsync(HttpContext context)
    {
        context.Request.EnableBuffering();
        using var reader = new StreamReader(
            context.Request.Body, Encoding.UTF8, false, 1024, true);

        var requestBody = await reader.ReadToEndAsync();
        context.Request.Body.Position = 0;

        _logger.LogInformation("Otrzymano żądanie: {Method} {Path} | Body: {Body}",
            context.Request.Method, context.Request.Path, requestBody);

        await _next(context);
    }
}



public class RabbitMqChannelPolicy : IPooledObjectPolicy<IModel>
{
    private readonly IConnection _connection;
    public RabbitMqChannelPolicy(IConnection connection) => _connection = connection;
    public IModel Create() => _connection.CreateModel();
    public bool Return(IModel obj)
    {
        if (obj.IsOpen) return true;
        obj.Dispose();
        return false;
    }
}

public interface IRabbitMqPublisher
{
    void PublishTelemetry(string routingKey, object message);
}

public class RabbitMqPublisher : IRabbitMqPublisher
{
    private readonly DefaultObjectPool<IModel> _channelPool;
    private const string ExchangeName = "telemetry_exchange";

    public RabbitMqPublisher(IConnection connection)
    {
        var policy = new RabbitMqChannelPolicy(connection);
        _channelPool = new DefaultObjectPool<IModel>(policy, maximumRetained: 50);

        using var channel = connection.CreateModel();
        channel.ExchangeDeclare(ExchangeName, "topic", true);
    }

    public void PublishTelemetry(string routingKey, object message)
    {
        var channel = _channelPool.Get();
        try
        {
            var json = JsonSerializer.Serialize(message);
            var body = Encoding.UTF8.GetBytes(json);
            var properties = channel.CreateBasicProperties();
            properties.Persistent = true;

            channel.BasicPublish(ExchangeName, routingKey, false, properties, body);
        }
        finally
        {
            _channelPool.Return(channel);
        }
    }
}

[ApiController]
[Route("api/[controller]")]
public class TelemetryController : ControllerBase
{
    private readonly IRabbitMqPublisher _publisher;
    private readonly ILogger<TelemetryController> _logger;

    public TelemetryController(IRabbitMqPublisher publisher, ILogger<TelemetryController> logger)
    {
        _publisher = publisher;
        _logger = logger;
    }

    [HttpPost("ingest")]
    public IActionResult IngestData([FromBody] TelemetryRequest request)
    {
        try
        {
            var base64EncodedBytes = Convert.FromBase64String(request.PayloadBase64);
            var decodedPayloadJson = Encoding.UTF8.GetString(base64EncodedBytes);

            var messageToPublish = new
            {
                DeviceId = request.DeviceId,
                Timestamp = request.Timestamp,
                DecodedData = decodedPayloadJson
            };

            string routingKey = $"telemetry.{request.DeviceId}";
            _publisher.PublishTelemetry(routingKey, messageToPublish);

            return Accepted(new { Message = "Dane przyjęte i przekazane do przetworzenia." });
        }
        catch (FormatException)
        {
            return BadRequest(new { Error = "Nieprawidłowy format Base64 w polu PayloadBase64." });
        }
    }
}