using System;
using Confluent.Kafka;

/**
    - Build Single Consumer Class (Consume Partition)
    - Poll Datas
*/

public class PartitionConsumer
{
    // Configuration Settings
    private readonly ConsumerConfig _config;
    private readonly string _topic;
    private readonly int _partition;
    private readonly long _offset;
    private IConsumer<Ignore, string> consumer;

    // Initialize
    public PartitionConsumer(ConsumerConfig config, string topic, int partition, long offset)
    {
        _config = config;
        _topic = topic;
        _partition = partition;
        _offset = offset;
    }

    public void Start()
    {
        ConsumeMessages();
    }

    public void ConsumeMessages()
    {
        var partition = new Partition(_partition);
        var topicPartition = new TopicPartition(_topic, partition);
        var topicPartitionOffset = new TopicPartitionOffset(topicPartition, new Offset(_offset));

        // string or ignore
        using var consumer = new ConsumerBuilder<string, string>(_config).Build();
        consumer.Assign(topicPartitionOffset);

        Console.WriteLine($"Consuming from topic {_topic}, partition {_partition}, starting from offset {_offset}");

        try
        {
            while (true)
            {
                var consumeResult = consumer.Consume();
                if (consumeResult == null) continue;

                Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Value}");

                // Optionally commit the offset manually
                consumer.Commit(consumeResult);
            }
        }
        catch (ConsumeException ex)
        {
            Console.WriteLine($"Consume error: {ex.Error.Reason}");
        }
        finally
        {
            consumer.Close();
        }
    }

    public void Stop()
    {
        consumer.Unassign();
        consumer.Close();
    }
}

public class Program
{
    public static void Main(string[] args)
    {
        var config = new ConsumerConfig
        {
            GroupId = "groupId",
            BootstrapServers = "bootstrapServers",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnablePartitionEof = true // Optionally enable EOF event
        };

        string topic = "topic";
        int partition = 0;
        long offset = 0;

        // Create Consumer
        var consumer = new PartitionConsumer(config, topic, partition, offset);
        consumer.Start();

        // Stop Consumer
        Console.WriteLine("Press any key to stop...");
        Console.ReadKey();
        consumer.Stop();
    }
}
