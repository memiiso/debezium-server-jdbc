# Debezium Jdbc Consumer

Replicates database CDC events to a jdbc database

## `Jdbc` Consumer

Jdbc consumer replicates debezium CDC events to destination Jdbc tables. It is possible to replicate source database one
to one or run it with append mode and keep all change events in jdbc table. When event and key schema
enabled (`debezium.format.value.schemas.enable=true`, `debezium.format.key.schemas.enable=true`) destination Jdbc
tables created automatically with initial job.

### Upsert

By default, Jdbc consumer is running with upsert mode `debezium.sink.jdbc.upsert=true`.
Upsert mode uses source Primary Key and does upsert on target table(delete followed by insert). For the tables without
Primary Key consumer falls back to append mode.

#### Data Deduplication

With upsert mode per batch data deduplication is done. Deduplication is done based on `__source_ts_ms` value and event
type `__op`.
its is possible to change field using `debezium.sink.jdbc.upsert-dedup-column=__source_ts_ms`. Currently only
Long field type supported.

Operation type priorities are `{"c":1, "r":2, "u":3, "d":4}`. When two records with same key and same `__source_ts_ms`
values received then the record with higher `__op` priority is kept and added to destination table and duplicate record
is dropped.

### Append

Setting `debezium.sink.jdbc.upsert=false` will set the operation mode to append. With append mode data deduplication is
not done and all received records are appended to destination table.
Note: For the tables without primary key operation mode falls back to append even configuration is set to upsert mode

#### Keeping Deleted Records

By default `debezium.sink.jdbc.upsert-keep-deletes=true` keeps deletes in the Jdbc table, setting it to false
will remove deleted records from the destination Jdbc table. With this config it's possible to keep last version of a
record in the destination Jdbc table(doing soft delete).

### Optimizing batch size (or commit interval)

Debezium extracts database events in real time and this could cause too frequent commits which is not optimal for batch
processing especially when near realtime data feed is sufficient. To avoid this problem following batch-size-wait
classes are used.

Batch size wait adds delay between consumer calls to increase total number of events received per call and meanwhile
events are collected in memory.
This setting should be configured together with `debezium.source.max.queue.size` and `debezium.source.max.batch.size`
debezium properties

#### NoBatchSizeWait

This is default configuration by default consumer will not use any wait. All the events are consumed immediately.

```properties
debezium.source.max.queue.size=16000
debezium.source.max.batch.size=2048
debezium.sink.batch.batch-size-wait=DynamicBatchSizeWait
debezium.sink.batch.batch-size-wait.max-wait-ms=5000
```

#### MaxBatchSizeWait

MaxBatchSizeWait uses debezium metrics to optimize batch size, this strategy is more precise compared to
DynamicBatchSizeWait.
MaxBatchSizeWait periodically reads streaming queue current size and waits until it reaches to `max.batch.size`.
Maximum wait and check intervals are controlled by `debezium.sink.batch.batch-size-wait.max-wait-ms`
, `debezium.sink.batch.batch-size-wait.wait-interval-ms` properties.

example setup to receive ~2048 events per commit. maximum wait is set to 30 seconds, streaming queue current size
checked every 5 seconds

```properties
debezium.sink.batch.batch-size-wait=MaxBatchSizeWait
debezium.sink.batch.metrics.snapshot-mbean=debezium.postgres:type=connector-metrics,context=snapshot,server=testc
debezium.sink.batch.metrics.streaming-mbean=debezium.postgres:type=connector-metrics,context=streaming,server=testc
debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector
debezium.source.max.batch.size=2048;
debezium.source.max.queue.size=16000";
debezium.sink.batch.batch-size-wait.max-wait-ms=30000
debezium.sink.batch.batch-size-wait.wait-interval-ms=5000
```

### Table Name Mapping

Jdbc tables are named by following rule : `table-prefix``database.server.name`_`database`_`table`

For example:

```properties
database.server.name=testc
debezium.sink.jdbc.table-prefix=cdc_
```

With above config database table = `inventory.customers` is replicated to `testc_cdc_inventory_customers`

## Debezium Event Flattening

Jdbc consumer requires event flattening.

```properties
debezium.transforms=unwrap
debezium.transforms.unwrap.type=io.debezium.transforms.ExtractNewRecordState
debezium.transforms.unwrap.add.fields=op,table,source.ts_ms,db
debezium.transforms.unwrap.add.headers=db
debezium.transforms.unwrap.delete.handling.mode=rewrite
```

### Configuring jdbc

All the properties starting with `debezium.sink.jdbc.__CONFIG__` are passed to Jdbc, and to hadoopConf

```properties
debezium.sink.jdbc.{jdbc.prop.name}=xyz-value # passed to jdbc!
```

### Example Configuration

Read [application.properties.example](../debezium-server-jdbc-sink/src/main/resources/conf/application.properties.example)
