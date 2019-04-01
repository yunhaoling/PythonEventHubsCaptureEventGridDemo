# PythonEventHubsCaptureEventGridDemo

The original c# version project address is https://github.com/Azure/azure-event-hubs/tree/master/samples/e2e/EventHubsCaptureEventGridDemo.

## Prerequisites

- Python 3.6
- Docker
- SQL Server Driver 13/17
- Read through the README of original project

## Steps

1. Follow the steps in https://github.com/Azure/azure-event-hubs/tree/master/samples/e2e/EventHubsCaptureEventGridDemo to deploy the infrastructure **except for the Functions App**.
2. Create a Function App on Azure. Choose Linux as the OS and Python as the Runtime Stack.
3. Follow the steps in https://docs.microsoft.com/en-us/azure/azure-functions/functions-create-first-function-python and https://docs.microsoft.com/en-us/azure/azure-functions/functions-reference-python to to publish the local **EventGridTriggerMigrateData** function to the Function App created on Azure.
    - If you have an existing resource group named myResourceGroup with any non-Linux App Service apps, you must use a different resource group. You can't host both Windows and Linux apps in the same resource group.
4. Configure the Application Settings of the Functions App. Set **StorageConnectionString** and **PythonSqlDwConnection**.
5. Add Event Grid subscription to the Functions App which is described in https://github.com/Azure/azure-event-hubs/tree/master/samples/e2e/EventHubsCaptureEventGridDemo.
6. Using your own **EVENTHUB_CONNECTION_STRING** and **EVENTHUB_NAME** in program.py.
7. Run program.py  to generate data

## References

1. Create your first Python function in Azure (preview): https://docs.microsoft.com/en-us/azure/azure-functions/functions-create-first-function-python

2. Azure Functions Python developer guide: https://docs.microsoft.com/en-us/azure/azure-functions/functions-reference-python
3. Migrate Captured Event Hubs data to a SQL Data Warehouse using Event Grid and Azure Function: https://github.com/Azure/azure-event-hubs/tree/master/samples/e2e/EventHubsCaptureEventGridDemo



