# DuckDB Kafka Extension

This extension allows you to consume Kafka messages through a DuckDB Table function that connects to a Kafka broker and streams messages as a table.

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
