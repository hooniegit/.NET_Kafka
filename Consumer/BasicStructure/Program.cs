using Confluent.Kafka;

/** 
    - Build Consumer Class
    - Poll Datas
    - Run Multi Tasks Using Threads
*/

class KafkaConsumer
{
    private IConsumer<Ignore, string> consumer;

    // Set Consumer Configs
    public KafkaConsumer(string bootstrapServers, string groupId, string topic)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        consumer.Subscribe(topic);
        Console.WriteLine($"[Consumer] Consuming Topic {topic}"); // Log
    }

    // Poll Datas
    public void PollDatas()
    {
        Console.WriteLine("[Consumer] Started. Press Ctrl+C to exit."); // Log

        try
        {
            while (true)
            {
                var consumeResult = consumer.Consume();

                // Run Multi Tasks
                Tasks(consumeResult);
            }
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            consumer.Close();
        }
    }

    // Config Multi Tasks
    private void Tasks(ConsumeResult<Ignore, string> consumeResult)
    {
        Console.WriteLine("[Consumer] Task Threads Started"); // Log

		// Create Tasks List
        List<Thread> threadList = new List<Thread>();

        // Add Tasks
        threadList.Add(new Thread(() => PrintResults(consumeResult)));
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
    private void PrintHello()
    {
        Console.WriteLine("Hello, Kafka!");
    }

}

class Program
{
    static void Main()
    {
        KafkaConsumer kafkaConsumer = new KafkaConsumer(
            "bootstrapServers",
            "groupId",
            "topic"
            );

        kafkaConsumer.PollDatas();
    }
}
