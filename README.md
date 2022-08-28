single tx writer
single tx writer+single non-tx writer
single tx writer+another tx writer
idempotent write requires a topic?
 - idempotence mode on and off, does producer.transaction.producerIdandEpoch exhibit any diff
what if aborted
timed out 
commit offset as well
multiple partition commit
how tx id is formed in streaming apps
a topology has two input topics, how is the tx id formed
what will happen next if a streaming app instance is fenced off

tx consumer
non-tx consumer

ProducerFencedException (https://tgrez.github.io/posts/2019-04-13-kafka-transactions.html)
tx timeout

does streaming app instances (using same app-id) use the same transactional-id or?

# Value of Kafka Transaction
## Prevent Reprocessing
We may reprocess the input message A, resulting in duplicate B messages being written to the output, violating the exactly-once processing semantics. Reprocessing may happen if the stream processing application crashes after writing B but before marking A as consumed. Thus when it resumes, it will consume A again and write B again, causing a duplicate.

## Zombie Fencing
In distributed environments, applications will crash or—worse!—temporarily lose connectivity to the rest of the system. Typically, new instances are automatically started to replace the ones which were deemed lost. Through this process, we may have multiple instances processing the same input topics and writing to the same output topics, causing duplicate outputs and violating the exactly-once processing semantics. We call this the problem of "zombie instances."

# Consumer
## read_uncommitted (default)
sees everything records no matter of whether transaction is committed or aborted?

## read_committed
1. read only committed transactional messages
2. read non-transactional messages that appear after committed transactional messages 

# Commit Marker & Offsets
- not for record count when TX are there

# Kafka Patterns 
Dedupe!

# Kafka Rebalancing

# Commands to monitor broker side 
1. Monitor `__transaction_state` topic
`kafka-console-consumer --consumer-property "exclude.internal.topics"="false" --topic __transaction_state --bootstrap-server localhost:9092 --formatter "kafka.coordinator.transaction.TransactionLog\$TransactionLogMessageFormatter"`
2. Consumer read_uncommitted
`kafka-console-consumer --consumer-property "isolation.level"="read_uncommitted" --topic transaction-demo --bootstrap-server localhost:9092`
3. Consumer read_committed 
`kafka-console-consumer --consumer-property "isolation.level"="read_committed" --topic transaction-demo --bootstrap-server localhost:9092`
4. Dump partition data log file 
`kafka-dump-log --files 00000000000000000000.log --print-data-log`
