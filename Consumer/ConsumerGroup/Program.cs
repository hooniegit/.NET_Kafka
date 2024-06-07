using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

/**
    - Build Multiple Consumer Classes
    - Poll Datas
    - Manage Consumer Threads with Exceptions
*/

public class KafkaConsumerManager
{
    // Configuration Settings
    private readonly ConcurrentDictionary<int, Task> _consumerTasks = new();
    private readonly int _consumerCount;
    private readonly ConsumerConfig _config;
    private readonly string _topic;
    private readonly CancellationTokenSource _cancellationTokenSource = new();

    // Initialize
    public KafkaConsumerManager(int consumerCount, ConsumerConfig config, string topic)
    {
        _consumerCount = consumerCount;
        _config = config;
        _topic = topic;
    }

    // Start Consumer Thread
    public void Start()
    {
        for (int i = 0; i < _consumerCount; i++)
        {
            StartConsumer(i);
        }
    }

    private void StartConsumer(int id)
    {
        var consumerTask = Task.Run(() => ConsumeMessages(id, _cancellationTokenSource.Token), _cancellationTokenSource.Token);
        _consumerTasks[id] = consumerTask;
    }

    // Consume Messages
    private void ConsumeMessages(int id, CancellationToken cancellationToken)
    {
        // Create Consumer & Subscribe Topic
        using var consumer = new ConsumerBuilder<string, string>(_config).Build();
        consumer.Subscribe(_topic);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                // Consume Messages
                var consumeResult = consumer.Consume(cancellationToken);
                if (consumeResult != null)
                {
                    // Insert Tasks Here..
                    Console.WriteLine($"[Consumer {id}] \nconsumed message : '{consumeResult.Message.Value}' \nat: '{consumeResult.TopicPartitionOffset}'.");
                }
            }
        }
        catch (OperationCanceledException)
        {
            consumer.Close();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Consumer {id} error: {ex.Message}");
            RestartConsumer(id);
        }
    }

    // Restart Consumer
    private void RestartConsumer(int id)
    {
        if (_consumerTasks.TryRemove(id, out var oldTask))
        {
            StartConsumer(id);
        }
    }

    // Stop Consumer
    public void Stop()
    {
        _cancellationTokenSource.Cancel();
        Task.WaitAll(_consumerTasks.Values.ToArray());
    }
}

class Program
{
    static void Main(string[] args)
    {
        // Set Configuration
        var config = new ConsumerConfig
        {
            GroupId = "groupId",
            BootstrapServers = "bootstrapServers",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        // Create & Manage Consumers
        var consumerManager = new KafkaConsumerManager(20, config, "topicName");
        consumerManager.Start();

        Console.WriteLine("Press any key to stop...");
        Console.ReadKey();

        consumerManager.Stop();
    }
}
