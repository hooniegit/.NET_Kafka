using System;
using System.IO;
using System.IO.Compression;
using System.Text;
using System.Threading;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Newtonsoft.Json.Linq;

/**
    - 
    - 
    - 
*/

public class KafkaConsumer
{
    private readonly string _bootstrapServers;
    private readonly string _topic;
    private readonly int _partitionNumber;
    private readonly string _groupId;

    // Initialize
    public KafkaConsumer(string bootstrapServers, string topic, int partitionNumber, string groupId = null)
    {
        _bootstrapServers = bootstrapServers;
        _topic = topic;
        _partitionNumber = partitionNumber;
        _groupId = groupId;
    }

    // Consume Data
    public void Start()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = _groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        // Create Consumer & Subscribe Topic
        using (var consumer = new ConsumerBuilder<Ignore, byte[]>(config).Build())
        {
            var partition = new TopicPartition(_topic, new Partition(_partitionNumber));
            consumer.Assign(partition);
            Console.WriteLine($"[Consumer] started for partition {_partitionNumber}."); // Log

            try
            {
                while (true)
                {
                    var consumeResult = consumer.Consume(CancellationToken.None);

                    // Consume Compressed Data
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
                            Console.WriteLine($"Received JSON data: {jsonData}");
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                consumer.Close();
            }
        }
    }

    // Create List of Decompressed Datas
    // 
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
}

public class KafkaAvroConsumer
{
    public static void Main(string[] args)
    {
        var config = new ConsumerConfig
        {
            GroupId = "avro-consumer-group",
            BootstrapServers = "localhost:9092",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        var schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = "localhost:8081" // Schema Registry URL
        };

        using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
        using (var consumer = new ConsumerBuilder<Ignore, MyAvroMessage>(config)
            .SetValueDeserializer(new AvroDeserializer<MyAvroMessage>(schemaRegistry).AsSyncOverAsync())
            .Build())
        {
            consumer.Subscribe("avro-topic");

            try
            {
                while (true)
                {
                    var consumeResult = consumer.Consume();

                    // Avro 메시지
                    var message = consumeResult.Message.Value;

                    Console.WriteLine($"Received message: {message.Property1}, {message.Property2}");
                }
            }
            catch (OperationCanceledException)
            {
                consumer.Close();
            }
        }
    }
}

public class MyAvroMessage
{
    public string Property1 { get; set; }
    public int Property2 { get; set; }
    // Add more properties as needed
}
