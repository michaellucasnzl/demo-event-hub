using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;
using Azure.Data.SchemaRegistry;
using Azure.Identity;
using Microsoft.Azure.Data.SchemaRegistry.ApacheAvro;
using Microsoft.Extensions.Configuration;
using models;

var builder = new ConfigurationBuilder()
    .AddUserSecrets<Program>();

IConfiguration configuration = builder.Build();

// Create a blob container client that the event processor will use 
var storageClient = new BlobContainerClient(
    configuration["StorageAccountConnectionString"],
    configuration["BlobContainerName"]);

// Create an event processor client to process events in the event hub
var processor = new EventProcessorClient(
    storageClient,
    EventHubConsumerClient.DefaultConsumerGroupName,
    configuration["EventHubConnectionString"],
    configuration["EventHubName"]);

// Register handlers for processing events and handling errors
processor.ProcessEventAsync += ProcessEventHandler;
processor.ProcessErrorAsync += ProcessErrorHandler;

// Start the processing
await processor.StartProcessingAsync();

// Wait for 30 seconds for the events to be processed
await Task.Delay(TimeSpan.FromSeconds(30));

// Stop the processing
await processor.StopProcessingAsync();

async Task ProcessEventHandler(ProcessEventArgs eventArgs)
{
    try
    {
        // Create a schema registry client that you can use to serialize and validate data.  
        var schemaRegistryClient =
            new SchemaRegistryClient(configuration["SchemaRegistryEndpoint"], new DefaultAzureCredential());

        // Create an Avro object serializer using the Schema Registry client object. 
        var serializer = new SchemaRegistryAvroSerializer(schemaRegistryClient, configuration["SchemaGroupName"],
            new SchemaRegistryAvroSerializerOptions { AutoRegisterSchemas = true });

        // Deserialized data in the received event using the schema 
        var vehicle = (Vehicle)await serializer.DeserializeAsync(eventArgs.Data, typeof(Vehicle));

        // Print the received event
        Console.WriteLine(
            $"Received Vehicle: {vehicle.Id} -- {vehicle.Name} -- {vehicle.Description} -- {vehicle.Colour}");

        await eventArgs.UpdateCheckpointAsync(eventArgs.CancellationToken);
    }
    catch (Exception e)
    {
        Console.WriteLine($"Exception occurred when trying to process a message: {e.Message}");
    }
}

static Task ProcessErrorHandler(ProcessErrorEventArgs eventArgs)
{
    // Write details about the error to the console window
    Console.WriteLine(
        $"\tPartition '{eventArgs.PartitionId}': an unhandled exception was encountered. This was not expected to happen.");
    Console.WriteLine(eventArgs.Exception.Message);
    return Task.CompletedTask;
}