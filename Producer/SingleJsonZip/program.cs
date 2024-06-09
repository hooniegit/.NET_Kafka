using Confluent.Kafka;
// using System;
// using System.Collections.Generic;
using System.Diagnostics;
using System.Text.Json;
// using System.Threading;

public class KafkaProducer
{
    private readonly string _bootstrapServers;
    private readonly string _topic;

    // Initialize
    public KafkaProducer(string bootstrapServers, string topic)
    {
        _bootstrapServers = bootstrapServers;
        _topic = topic;
    }

    // Produce Message
    public void Produce(List<string> messages)
    {
        // Set Configuration
        var config = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers,
            CompressionType = CompressionType.Gzip // Set Compression Type
        };

        // Publish Message (Async)
        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {
            foreach (var message in messages)
            {
                try
                {
                    var dr = producer.ProduceAsync(_topic, new Message<Null, string> { Value = message }).Result;
                    // Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Delivery Failed: {e.Error.Reason}");
                }
            }
        }
    }
}

public class DataGenerator
{
    private static readonly Random random = new Random();

    // Generate List of JSON Serialized Datas
    public static List<string> GenerateData()
    {
        var dataList = new List<string>();
        string location = "LocationX";
        string source = "SourceY";

        for (int i = 1; i <= 20; i++)
        {
            string id = $"Loc{i:D2}";

            var data = new
            {
                location = location,
                source = source,
                value = GenerateValue(id),
                timestamp = DateTime.UtcNow
            };

            dataList.Add(JsonSerializer.Serialize(data));
        }

        return dataList;
    }

    // Generate Random Data
    private static double GenerateValue(string id)
    {
        int locNumber = int.Parse(id.Substring(3));
        double minValue = locNumber * 10;
        double maxValue = minValue + 5;

        return Math.Round(minValue + random.NextDouble() * (maxValue - minValue), 2);
    }
}

class Program
{
    static void Main(string[] args)
    {
        // Create Producer
        string bootstrapServers = "localhost:9092";
        string topic = "TestTopic";
        KafkaProducer producer = new KafkaProducer(bootstrapServers, topic);

        // Message
        Console.WriteLine("Press any key to stop...");

        // Set ReadKey Option
        bool running = true;
        Thread inputThread = new Thread(() =>
        {
            Console.ReadKey();
            running = false;
        });
        inputThread.Start();

        // Run Repeatable Tasks
        while (running)
        {
            // Start Stop Watch
            Stopwatch stopwatch = Stopwatch.StartNew();

            // Run Tasks
            List<string> dataList = DataGenerator.GenerateData();
            producer.Produce(dataList);
            Console.WriteLine("Data production completed.");

            // Stop Stop Watch
            stopwatch.Stop();

            // Count Time & Sleep - Check 1 Sec
            long elapsedMilliseconds = stopwatch.ElapsedMilliseconds;
            int sleepTime = (int)(1000 - elapsedMilliseconds);
            Console.WriteLine($@"Spent Time >> {elapsedMilliseconds.ToString()}");
            if (sleepTime > 0)
            {
                Thread.Sleep(sleepTime);
            }
        }
        inputThread.Join();
    }
}