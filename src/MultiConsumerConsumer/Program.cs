// See https://aka.ms/new-console-template for more information
using Confluent.Kafka;
using Confluent.Kafka.Admin;

const int commitPeriod = 5;
var server = "kafka1:9092";
var topics = new string[] { "topic-2", "topic-3" };

Console.WriteLine("Hello, World!");

async Task CreateTopics()
{
    using var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = server }).Build();
    try{
    await adminClient.CreateTopicsAsync(topics.Select(x => new TopicSpecification() { Name = x, NumPartitions = 4, ReplicationFactor = 1}));
    }
}
async Task Produce()
{
    var config = new ProducerConfig { BootstrapServers = "kafka1:9092" };
    using var producer = new ProducerBuilder<string, string>(config).Build();
    for (var i = 0; i < 10; i++)
    {
        var topic = topics[i % 2];
        await producer.ProduceAsync(topic, new Message<string, string> { Key = $"val - {i}", Value = $"val - {i}" });
        Console.WriteLine($"Produce Message to TOPIC {topic}");
    }
}

async Task Consume(string topic)
{
    var config = new ConsumerConfig
    {
        BootstrapServers = server,
        GroupId = "csharp-consumer2",
        StatisticsIntervalMs = 5000,
        SessionTimeoutMs = 6000,
        AutoOffsetReset = AutoOffsetReset.Latest,
    };
    await Task.Yield();
    using var consumer = new ConsumerBuilder<string, string>(config).Build();
    Console.WriteLine($"Run Consumer at topic: {topic}");
    consumer.Subscribe(topic);
    while (true)
    {
        try
        {
            var consumeResult = consumer.Consume();

            // if (consumeResult.IsPartitionEOF)
            // {
            //     Console.WriteLine(
            //         $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");

            //     continue;
            // }

            Console.WriteLine($"Received message from topic {consumeResult.Topic} at {consumeResult.TopicPartitionOffset}: {consumeResult.Message.Value}");

            // if (consumeResult.Offset % commitPeriod == 0)
            // {
            //     // The Commit method sends a "commit offsets" request to the Kafka
            //     // cluster and synchronously waits for the response. This is very
            //     // slow compared to the rate at which the consumer is capable of
            //     // consuming messages. A high performance application will typically
            //     // commit offsets relatively infrequently and be designed handle
            //     // duplicate messages in the event of failure.
            //     try
            //     {
            //         consumer.Commit(consumeResult);
            //     }
            //     catch (KafkaException e)
            //     {
            //         Console.WriteLine($"Commit error: {e.Error.Reason}");
            //     }
            // }
        }
        catch (ConsumeException e)
        {
            Console.WriteLine($"Consume error: {e.Error.Reason}");
        }
    }
    Console.WriteLine($"STOP");
    consumer.Close();
}

await CreateTopics();

var consumers = topics.Select(t => Consume(t));

var producer = Produce();
await producer;
await Task.WhenAll(consumers);
