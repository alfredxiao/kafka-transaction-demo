# Features
- single tx writer
- interleaved: tx writer + non-tx writer + another tx writer
- idempotent writer
- transaction abort
- transaction timed out
- [todo] commit offset as well
- [todo] multiple partition commit
- [todo] how tx id is formed in streaming apps
  - a topology has two input topics, how is the tx id formed
  - does streaming app instances (using same app-id) use the same transactional-id or?
- [todo] commit interval, or batch size in streaming app
  - how sparse or dense are tx markers in the output topic?
- [todo] does and how does rocksdb participate in exactly-once semantics
- [todo] what will happen next if a streaming app instance is fenced off
- [todo] TransactionAbortedTransaction?
- Zombie Fencing & ProducerFencedException (ref: https://tgrez.github.io/posts/2019-04-13-kafka-transactions.html)
- observed results from `__transaction_state` and tx markers

# Value of Kafka Transaction
## Prevent Reprocessing
We may reprocess the input message A, resulting in duplicate B messages being written to the output, violating the exactly-once processing semantics. Reprocessing may happen if the stream processing application crashes after writing B but before marking A as consumed. Thus when it resumes, it will consume A again and write B again, causing a duplicate.

## Zombie Fencing
In distributed environments, applications will crash or—worse!—temporarily lose connectivity to the rest of the system. Typically, new instances are automatically started to replace the ones which were deemed lost. Through this process, we may have multiple instances processing the same input topics and writing to the same output topics, causing duplicate outputs and violating the exactly-once processing semantics. We call this the problem of "zombie instances."

# Consumer
## read_uncommitted (default)
- sees all records despite transactional or not, committed, or aborted
- sees all records instantly (by producer via `send()` and its sending job), no need to wait for tx marker

## read_committed
1. reads only committed transactional messages
2. reads non-transactional messages if earlier messages are readable
3. read records in the order of they were sent (via `send()`), not the order of they were committed

# Transaction Marker & Offsets
- With tx marker's existence, offset is not appropriate for record count of a topic

# Commands to monitor broker side 
1. Monitor `__transaction_state` topic
`kafka-console-consumer --consumer-property "exclude.internal.topics"="false" --topic __transaction_state --bootstrap-server localhost:9092 --formatter "kafka.coordinator.transaction.TransactionLog\$TransactionLogMessageFormatter"`
2. Consumer read_uncommitted
`kafka-console-consumer --consumer-property "isolation.level"="read_uncommitted" --topic transaction-demo --bootstrap-server localhost:9092`
3. Consumer read_committed 
`kafka-console-consumer --consumer-property "isolation.level"="read_committed" --topic transaction-demo --bootstrap-server localhost:9092`
4. Dump partition data log file 
`kafka-dump-log --files 00000000000000000000.log --print-data-log`
