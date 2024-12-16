# DuckDB Kafka Extension

This extension allows you to consume Kafka messages through a DuckDB Table function that connects to a Kafka broker and streams messages as a table.

> The extension is a WIP and not functional! Join if you're willing to contribute!

## Examples

#### Basic usage:
```sql
SELECT * FROM kafka_consumer('localhost:9092', 'test-topic', 'test-group');
```

#### Secure usage:

```sql
SELECT * FROM kafka_consumer(
    'broker.some.cloud:9092', 
    'test-topic',
    'test-group',
    security_protocol := 'SASL_SSL',
    sasl_mechanism := 'PLAIN',
    username := 'your-key',
    password := 'your-secret'
);
```
