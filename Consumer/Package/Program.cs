using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

/**
    - Build Multiple Consumer Classes
    - Poll Datas
    - Manage Consumer Threads with Exceptions

    ※ Need to Merge Options Based on Here
*/

public class ConsumerGroup
{
    // Configuration Settings
    private readonly ConcurrentDictionary<int, Task> _consumerTasks = new();
    private readonly int _consumerCount;
    private readonly ConsumerConfig _config;
    private readonly string _topic;
    private readonly CancellationTokenSource _cancellationTokenSource = new();

    // Initialize
    public ConsumerGroup(int consumerCount, ConsumerConfig config, string topic)
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
        // Run Consumer Threads
        var consumerTask = Task.Run(() => ConsumeMessages(id, _cancellationTokenSource.Token), _cancellationTokenSource.Token);
        _consumerTasks[id] = consumerTask;
    }

    // Consume Messages
    private void ConsumeMessages(int id, CancellationToken cancellationToken)
    {
        // Create Consumer & Subscribe Topic
        // Set Key & Value Type as String
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
                    // Decompress Compressed Data
                    byte[] compressedData = consumeResult.Message.Value;
                    var decompressedFiles = DecompressData(compressedData);

                    foreach (var fileContent in decompressedFiles)
                    {
                        // Input Deserialize Logics Here..
                        // Aspect JSON Data
                        JObject jsonData = JObject.Parse(fileContent);

                        // Run Tasks
                        TasksTest(id, consumeResult);
                        Tasks(consumeResult, id);
                        Console.WriteLine($"[Consumer {id}] \nconsumed message : '{consumeResult.Message.Value}' \nat: '{consumeResult.TopicPartitionOffset}'.");

                    }
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

    private static List<string> DecompressData(byte[] data)
    {
        var fileContents = new List<string>();

        // 
        using (var compressedStream = new MemoryStream(data))
        using (var zipArchive = new ZipArchive(compressedStream, ZipArchiveMode.Read))
        {
            // 
            foreach (var entry in zipArchive.Entries)
            {
                using (var entryStream = entry.Open())
                using (var reader = new StreamReader(entryStream, Encoding.UTF8))
                {
                    fileContents.Add(reader.ReadToEnd());
                }
            }
        }

        return fileContents;
    }

    // Input Functions from External - Need to Test
    public delegate void SingleTask(ConsumeResult<Ignore, string> consumeResult, int consumerId);
    private void TasksTest(ConsumeResult<Ignore, string> consumeResult, int consumerId, List<SingleTask> TaskList)
    {
        Console.WriteLine("[Consumer] Task Threads Started"); // Log

		// Create Tasks List
        List<Thread> threadList = new List<Thread>();
        foreach (SingleTask currentTask in TaskList)
        {
            threadList.Add(new Thread(() => currentTask(consumerId, consumeResult)));
        }

        // Run Threads
        foreach (var thread in threadList)
        {
            thread.Start();
        }

        // Join Threads
        foreach (var thread in threadList)
        {
            thread.Join();
        }
        Console.WriteLine("[Consumer] Task Threads Finished"); // Log
    }


    // Config Multi Tasks
    private void Tasks(ConsumeResult<Ignore, string> consumeResult, int consumerId)
    {
        Console.WriteLine("[Consumer] Task Threads Started"); // Log

		// Create Tasks List
        List<Thread> threadList = new List<Thread>();

        // Add Tasks Here..
        threadList.Add(new Thread(() => PrintResults(consumeResult)));
        threadList.Add(new Thread(() => PrintThreads(consumerId)));
        threadList.Add(new Thread(PrintHello));

        // Run Threads
        foreach (var thread in threadList)
        {
            thread.Start();
        }

        // Join Threads
        foreach (var thread in threadList)
        {
            thread.Join();
        }
        Console.WriteLine("[Consumer] Task Threads Finished"); // Log
    }

    // EXAMPLE FUNCTION
    private void PrintResults(ConsumeResult<Ignore, string> consumeResult)
    {
        Console.WriteLine($@"Received message: 
Key: {consumeResult.Message.Key}, 
Value: {consumeResult.Message.Value},
Topic: {consumeResult.Topic},
Message: {consumeResult.Message.Value}, 
Partition: {consumeResult.Partition}, 
Offset: {consumeResult.Offset},
Timestamp: {consumeResult.Message.Timestamp.UnixTimestampMs}");
    }

    // EXAMPLE FUNCTION
    private void PrintThreads(int consumerId)
    {
        
    }

    // EXAMPLE FUNCTION
    private void PrintHello()
    {

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

        int partitionNumber = 20;
        string topic = "topic";

        // Create & Manage Consumers
        var consumerManager = new KafkaConsumer(partitionNumber, config, topic);
        consumerManager.Start();

        // Stop Consumer
        Console.WriteLine("Press any key to stop...");
        Console.ReadKey();
        consumerManager.Stop();
    }
}
