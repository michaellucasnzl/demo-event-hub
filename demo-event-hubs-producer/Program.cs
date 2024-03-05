using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Azure.Data.SchemaRegistry;
using Azure.Identity;
using Microsoft.Azure.Data.SchemaRegistry.ApacheAvro;
using Microsoft.Extensions.Configuration;
using models;

var builder = new ConfigurationBuilder()
    .AddUserSecrets<Program>();

IConfiguration configuration = builder.Build();

// The Event Hubs client types are safe to cache and use as a singleton for the lifetime
// of the application, which is best practice when events are being published or read regularly.
EventHubProducerClient producerClient = new EventHubProducerClient(
    configuration["EventHubNamespaceConnectionString"],
    configuration["EventHubName"]);

// Create a schema registry client that you can use to serialize and validate data.  
var schemaRegistryClient =
    new SchemaRegistryClient(configuration["SchemaRegistryEndpoint"], new DefaultAzureCredential());

// Create an Avro object serializer using the Schema Registry client object. 
var serializer = new SchemaRegistryAvroSerializer(schemaRegistryClient, configuration["SchemaGroupName"],
    new SchemaRegistryAvroSerializerOptions { AutoRegisterSchemas = true });

// Create event data
var vehicle = new Vehicle()
{
    Id = 1,
    Name = "Best Car",
    Description = "Best car in the world",
    Colour = "Gold"
};
var eventData = (EventData)await serializer.SerializeAsync(vehicle, messageType: typeof(EventData));

// Create a batch of events 
using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();

// Add the event data to the event batch. 
eventBatch.TryAdd(eventData);

// Send the batch of events to the event hub. 
await producerClient.SendAsync(eventBatch);
Console.WriteLine("A batch of 1 order has been published.");