<img src="https://github.com/user-attachments/assets/46a5c546-7e9b-42c7-87f4-bc8defe674e0" width=250 />

# DuckDB Kafka Extension

This extension can be used to consume Kafka messages from brokers through a DuckDB Table function.

> The extension is a WIP and not functional! Join if you're willing to contribute!

## Examples

#### Basic usage:
```sql
SELECT * FROM kafquack('localhost:9092', 'test-topic', 'test-group');
```

#### Secure usage:

```sql
SELECT * FROM kafquack(
    'broker.some.cloud:9092', 
    'test-topic',
    'test-group',
    security_protocol := 'SASL_SSL',
    sasl_mechanism := 'PLAIN',
    username := 'your-key',
    password := 'your-secret'
);
```

#### View
```sql
-- Create a view that consumes messages from Kafka
CREATE VIEW kafka_messages AS 
SELECT * FROM kafquack('localhost:9092', 'test-topic', 'test-group');

-- Query messages
SELECT * FROM kafka_messages;
```
