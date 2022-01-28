// See https://aka.ms/new-console-template for more information
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Cocona;
using Cocona.Application;

const int commitPeriod = 5;
var server = "kafka1:9092";
var topics = new string[] { "topic-2", "topic-3" };

Console.WriteLine("Hello, World!");


var builder = CoconaApp.CreateBuilder();

var app = builder.Build();


app.AddCommand("create-topic", async (string topic, CoconaAppContext ctx) =>
{
    Console.WriteLine($"Running...");
    await CreateTopic(topic, ctx.CancellationToken);
    Console.WriteLine($"Done.");
});

app.AddCommand("produce", async (string topic, CoconaAppContext ctx) =>
{
    Console.WriteLine($"Running...");
    await Produce(topic, ctx.CancellationToken);
    Console.WriteLine($"Done.");
});

app.AddCommand("consume", async (string topic, CoconaAppContext ctx) =>
{
    Console.WriteLine($"Running...");
    await Consume(topic, ctx.CancellationToken);
    Console.WriteLine($"Done.");
});

async Task CreateTopic(string topic, CancellationToken cancellationToken)
{
    using var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = server }).Build();
    try
    {
        await adminClient.CreateTopicsAsync(new []{new TopicSpecification() { Name = topic, NumPartitions = 4, ReplicationFactor = 1 }});
    }
    catch(Exception ex)
    {
        Console.WriteLine(ex);
    }
}
async Task Produce(string topic, CancellationToken cancellationToken)
{
    var config = new ProducerConfig { BootstrapServers = "kafka1:9092" };
    using var producer = new ProducerBuilder<Null, string>(config).Build();
    for (var i = 0; i < 10; i++)
    {
        await producer.ProduceAsync(topic, new Message<Null, string> { Value = $"val - {i}" });
        Console.WriteLine($"Produce Message to TOPIC {topic}");
    }
}

async Task Consume(string topic, CancellationToken cancellationToken)
{
    var config = new ConsumerConfig
    {
        BootstrapServers = server,
        GroupId = $"csharp-consumer-{topic}",
        SessionTimeoutMs = 6000,
        AutoOffsetReset = AutoOffsetReset.Latest,
        EnableAutoCommit=true,
        EnablePartitionEof = true
    };
    await Task.Yield();
    using var consumer = new ConsumerBuilder<Null, string>(config)
                .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e}"))
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    Console.WriteLine($"ConsumerId: {c.MemberId}, Assigned partitions: {string.Join(',', partitions)}]");
                })
                .SetPartitionsRevokedHandler((c, partitions) =>
                {
                    Console.WriteLine($"ConsumerId: {c.MemberId}, Revoking assignment: {partitions}");
                }).Build();
    Console.WriteLine($"Run Consumer at topic: {topic}");
    consumer.Subscribe(topic);
    while (true)
    {
        try
        {
            var consumeResult = consumer.Consume();

            if (consumeResult.IsPartitionEOF)
            {
                Console.WriteLine(
                    $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");

                continue;
            }

            Console.WriteLine($"Received message from topic {consumeResult.Topic} at {consumeResult.TopicPartitionOffset}: {consumeResult.Message.Value}");

            if (consumeResult.Offset % commitPeriod == 0)
            {
                // The Commit method sends a "commit offsets" request to the Kafka
                // cluster and synchronously waits for the response. This is very
                // slow compared to the rate at which the consumer is capable of
                // consuming messages. A high performance application will typically
                // commit offsets relatively infrequently and be designed handle
                // duplicate messages in the event of failure.
                try
                {
                    consumer.Commit(consumeResult);
                }
                catch (KafkaException e)
                {
                    Console.WriteLine($"Commit error: {e.Error.Reason}");
                }
            }
        }
        catch (ConsumeException e)
        {
            Console.WriteLine($"Consume error: {e.Error.Reason}");
        }
        catch (Exception e)
        {
            Console.WriteLine($"Consume error: {e}");
        }
    }
    Console.WriteLine($"STOP");
    consumer.Close();
}

app.Run();