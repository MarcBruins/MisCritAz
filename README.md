# MisCritAz
Sample code that uses 2 services buses and 2 storage accounts. PoC only.

## What is this?

This project can be used as a proof of concept. It demonstrates a way to use a primary and secondary Azure Service Bus & Azure Blob Storage account.
A circuit breaker is added to determine which instance to use.

## Challenges

### Get the Service Bus code running
Create 2 Service Bus (Standard) resources in your Azure test subscription. Create topic named 'testtopic', create subscription named 'testsubscription' in both.
Add the connection settings to the configuration.
Run the code, use the debugger to close the primary client during a send operation. Notice that the secondary client will be used.
Notice that the circuit breaker opens for a while. 
And finally, notice that everything returns to normal eventually.

Did you use connection string with limited access? (Send only / receive only)
Think of a way to maintain message ordering when using 2 topics.

### Get the Blob storage code running
Create 2 Storage Account resources in your Azure test subscription.
Add the connection settings to the configuration.
Run the code, use the debugger to set the primary client to null during a write operation. Notice that the secondary client will be used.
Notice that the circuit breaker opens for a while. 
And finally, notice that everything returns to normal eventually.

Think of a way to move the blobs from the primary to the secondary storage account.

### Bonus challenge
Deploy Azure SQL Server (Standard)
Enable geo replication
Configure a Failover Group

Notice that the resolved endpoint behind the group DNS name changes, after you perform a fail-over. 
