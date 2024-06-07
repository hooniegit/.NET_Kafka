using System;
using Confluent.Kafka;
using Newtonsoft.Json;

/**
    - 
    - 
    - 
*/

public class KafkaJsonConsumer
{
    public static void Main(string[] args)
    {
        var config = new ConsumerConfig
        {
            GroupId = "json-consumer-group",
            BootstrapServers = "localhost:9092",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
        {
            consumer.Subscribe("json-topic");

            try
            {
                while (true)
                {
                    var consumeResult = consumer.Consume();
                    var message = JsonConvert.DeserializeObject<MyMessage>(consumeResult.Message.Value);

                    Console.WriteLine($"Received message: {message}");
                }
            }
            catch (OperationCanceledException)
            {
                consumer.Close();
            }
        }
    }
}

public class MyMessage
{
    // Insert Properties Here..
    public string Property1 { get; set; }
    public int Property2 { get; set; }
}

using System;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

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
    // Insert Properties Here..
    public string Property1 { get; set; }
    public int Property2 { get; set; }
}
