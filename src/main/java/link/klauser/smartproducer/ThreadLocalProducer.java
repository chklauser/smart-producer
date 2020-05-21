package link.klauser.smartproducer;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serializer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 * A simplification around using Kafka from a thread pool. A single {@link ThreadLocalProducer} delegates
 * to a different producer on each thread (thread-local object).
 * </p>
 * <p>
 *     The {@link #close(Duration)} method gets applied to <em>all</em> producers. As a result, the
 *     {@link #close(Duration)} method <strong>must not be called while threads are still actively using this
 *     {@link ThreadLocalProducer}</strong>.
 * </p>
 * @param <K>
 * @param <V>
 */
@SuppressWarnings("java:S5164" /* thread local is not cleaned up */)
@RequiredArgsConstructor
class ThreadLocalProducer<K,V> implements Producer<K, V>, AutoCloseable {
    @Getter
    @NonNull
    private final Properties properties;

    @Getter
    private final Serializer<K> keySerializer;

    @Getter
    private final Serializer<V> valueSerializer;

    private final ThreadLocal<Producer<K,V>> local = new ThreadLocal<>();
    private final ConcurrentLinkedQueue<KafkaProducer<K,V>> allProducers = new ConcurrentLinkedQueue<>();

    private static final AtomicInteger nextId = new AtomicInteger(0);

    @NonNull
    private Producer<K,V> getLocal() {
        var localProducer = local.get();
        if(localProducer == null){
            Properties localProperties = new Properties();
            localProperties.putAll(properties);
            localProperties.put(ProducerConfig.CLIENT_ID_CONFIG,
                    localProperties.get(ProducerConfig.CLIENT_ID_CONFIG).toString() + nextId.getAndIncrement());
            var kafkaProducer = new KafkaProducer<>(localProperties, keySerializer, valueSerializer);
            local.set(kafkaProducer);
            allProducers.add(kafkaProducer);
            return kafkaProducer;
        }
        else {
            return localProducer;
        }
    }

    @Override
    public void initTransactions() {
        getLocal().initTransactions();
    }

    @Override
    public void beginTransaction() {
        getLocal().beginTransaction();
    }

    @Override
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId) {
        getLocal().sendOffsetsToTransaction(offsets, consumerGroupId);
    }

    @Override
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, ConsumerGroupMetadata groupMetadata) {
        getLocal().sendOffsetsToTransaction(offsets, groupMetadata);
    }

    @Override
    public void commitTransaction() {
        getLocal().commitTransaction();
    }

    @Override
    public void abortTransaction() {
        getLocal().abortTransaction();
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return getLocal().send(record);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        return getLocal().send(record, callback);
    }

    @Override
    public void flush() {
        getLocal().flush();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return getLocal().partitionsFor(topic);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return getLocal().metrics();
    }

    @Override
    public void close() {
        close(Duration.ofMillis(Long.MAX_VALUE));
    }

    @Override
    public void close(Duration timeout) {
        while(true){
            var liveProducer = allProducers.poll();
            if(liveProducer == null){
                break;
            }
            liveProducer.close(timeout);
        }
    }
}
