using Confluent.Kafka;
using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

class Program
{
    public static async Task Main(string[] args)
    {
        string tokenEndpoint = "<https://oauth-server/token>";
        string clientId = "client-id";
        string clientSecret = "client-secret";

        var tokenProvider = new OAuthTokenProvider(tokenEndpoint, clientId, clientSecret);
        var token = await tokenProvider.GetOAuthTokenAsync();

        var consumerTask = KafkaConsumer.StartConsumerAsync(tokenProvider, token);

        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true; // prevent the process from terminating.
            tokenProvider.Cancel();
        };

        await consumerTask;
    }
}

class KafkaConsumer
{
    public static async Task StartConsumerAsync(OAuthTokenProvider tokenProvider, TokenResponse token)
    {
        var config = new ConsumerConfig
        {
            GroupId = "test-consumer-group",
            BootstrapServers = "workspace:9093",
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.OAuthBearer,
            SaslOauthbearerToken = token.AccessToken,
            SslCaLocation = "path/to/ca-cert.pem",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
        {
            consumer.Subscribe("your-secure-topic");

            var cts = new CancellationTokenSource();
            tokenProvider.SetCancellationTokenSource(cts);

            var tokenRefreshTask = RefreshTokenPeriodicallyAsync(tokenProvider, consumer);

            try
            {
                while (true)
                {
                    try
                    {
                        var cr = consumer.Consume(cts.Token);
                        Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Consume error: {e.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                consumer.Close();
            }
            finally
            {
                cts.Cancel();
                await tokenRefreshTask;
            }
        }
    }

    private static async Task RefreshTokenPeriodicallyAsync(OAuthTokenProvider tokenProvider, IConsumer<Ignore, string> consumer)
    {
        while (!tokenProvider.IsCancellationRequested())
        {
            var token = await tokenProvider.GetOAuthTokenAsync();
            consumer.UpdateOAuthBearerToken(token.AccessToken);

            await Task.Delay((token.ExpiresIn - 10) * 1000);
        }
    }
}

class OAuthTokenProvider
{
    private readonly string _tokenEndpoint;
    private readonly string _clientId;
    private readonly string _clientSecret;
    private CancellationTokenSource _cts;

    public OAuthTokenProvider(string tokenEndpoint, string clientId, string clientSecret)
    {
        _tokenEndpoint = tokenEndpoint;
        _clientId = clientId;
        _clientSecret = clientSecret;
    }

    public void SetCancellationTokenSource(CancellationTokenSource cts)
    {
        _cts = cts;
    }

    public void Cancel()
    {
        _cts?.Cancel();
    }

    public bool IsCancellationRequested()
    {
        return _cts?.IsCancellationRequested ?? false;
    }

    public async Task<TokenResponse> GetOAuthTokenAsync()
    {
        using (var client = new HttpClient())
        {
            var request = new HttpRequestMessage(HttpMethod.Post, _tokenEndpoint);
            var credentials = new StringContent($"grant_type=client_credentials&client_id={_clientId}&client_secret={_clientSecret}");
            request.Content = credentials;
            request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/x-www-form-urlencoded");

            var response = await client.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var responseContent = await response.Content.ReadAsStringAsync();
            return JsonSerializer.Deserialize<TokenResponse>(responseContent);
        }
    }
}

class TokenResponse
{
    public string AccessToken { get; set; }
    public int ExpiresIn { get; set; }
}
