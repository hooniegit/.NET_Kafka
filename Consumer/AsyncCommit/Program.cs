using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace KafkaConsumerExample
{
    public class KafkaConsumer
    {
        private readonly string _brokerList;
        private readonly string _topic;
        private readonly string _groupId;

        public KafkaConsumer(string brokerList, string topic, string groupId)
        {
            _brokerList = brokerList;
            _topic = topic;
            _groupId = groupId;
        }

        public void Start()
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = _brokerList,
                GroupId = _groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };

            using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                consumer.Subscribe(_topic);
                CancellationTokenSource cts = new CancellationTokenSource();

                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };

                try
                {
                    while (!cts.Token.IsCancellationRequested)
                    {
                        try
                        {
                            var consumeResult = consumer.Consume(cts.Token);

                            // Insert Tasks Here..
                            // Process the message
                            Console.WriteLine($"Consumed message '{consumeResult.Message.Value}' at: '{consumeResult.TopicPartitionOffset}'.");

                            // Asynchronous commit using Task.Run
                            Task.Run(() =>
                            {
                                try
                                {
                                    consumer.Commit(consumeResult);
                                }
                                catch (Exception asyncEx)
                                {
                                    Console.WriteLine($"CommitAsync error: {asyncEx.Message}");
                                }
                            });
                        }
                        catch (ConsumeException ex)
                        {
                            Console.WriteLine($"Consume error: {ex.Error.Reason}");
                        }
                    }

                    // Synchronous commit outside the while loop for final offsets
                    try
                    {
                        consumer.Commit();
                    }
                    catch (Exception syncEx)
                    {
                        Console.WriteLine($"CommitSync error: {syncEx.Message}");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error: {ex.Message}");
                }
                finally
                {
                    // Ensure the consumer leaves the group cleanly and final offsets are committed.
                    consumer.Close();
                }
            }
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            // Kafka configuration
            string brokerList = "localhost:9092";
            string topic = "test_topic";
            string groupId = "test_group";

            // Initialize and start the Kafka consumer
            KafkaConsumer consumer = new KafkaConsumer(brokerList, topic, groupId);
            consumer.Start();
        }
    }

}
