using System;
using Confluent.Kafka;

/**
    - Build Single Consumer Class (Consume Partition)
    - 
    - 
    - 
*/

public class PartitionConsumer
{
    private readonly string _brokerList;
    private readonly string _topic;
    private readonly int _partition;
    private readonly long _offset;
    private IConsumer<Ignore, string> _consumer;

    public PartitionConsumer(string brokerList, string topic, int partition, long offset)
    {
        _brokerList = brokerList;
        _topic = topic;
        _partition = partition;
        _offset = offset;

        var config = new ConsumerConfig
        {
            BootstrapServers = _brokerList,
            GroupId = Guid.NewGuid().ToString(), // Unique group id for this consumer instance
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnablePartitionEof = true // Optionally enable EOF event
        };

        _consumer = new ConsumerBuilder<Ignore, string>(config).Build();
    }

    public void StartConsuming()
    {
        var partition = new Partition(_partition);
        var topicPartition = new TopicPartition(_topic, partition);
        var topicPartitionOffset = new TopicPartitionOffset(topicPartition, new Offset(_offset));

        _consumer.Assign(topicPartitionOffset);

        Console.WriteLine($"Consuming from topic {_topic}, partition {_partition}, starting from offset {_offset}");

        try
        {
            while (true)
            {
                var result = _consumer.Consume();
                if (result == null) continue;

                Console.WriteLine($"Received message at {result.TopicPartitionOffset}: {result.Value}");

                // Optionally commit the offset manually
                _consumer.Commit(result);
            }
        }
        catch (ConsumeException ex)
        {
            Console.WriteLine($"Consume error: {ex.Error.Reason}");
        }
        finally
        {
            _consumer.Close();
        }
    }

    public void StopConsuming()
    {
        _consumer.Unassign();
        _consumer.Close();
    }
}

public class Program
{
    public static void Main(string[] args)
    {
        string brokerList = "localhost:9092";
        string topic = "test-topic";
        int partition = 0;
        long offset = 0; // Starting offset

        var consumer = new PartitionConsumer(brokerList, topic, partition, offset);
        consumer.StartConsuming();
    }
}
