using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;
using System.Text;
using Microsoft.Extensions.Configuration;

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

Task ProcessEventHandler(ProcessEventArgs eventArgs)
{
    // Write the body of the event to the console window
    Console.WriteLine("\tReceived event: {0}", Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray()));
    Console.ReadLine();
    return Task.CompletedTask;
}

Task ProcessErrorHandler(ProcessErrorEventArgs eventArgs)
{
    // Write details about the error to the console window
    Console.WriteLine(
        $"\tPartition '{eventArgs.PartitionId}': an unhandled exception was encountered. This was not expected to happen.");
    Console.WriteLine(eventArgs.Exception.Message);
    Console.ReadLine();
    return Task.CompletedTask;
}