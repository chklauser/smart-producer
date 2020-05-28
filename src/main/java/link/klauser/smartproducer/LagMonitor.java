package link.klauser.smartproducer;

import lombok.*;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import me.tongfei.progressbar.ProgressBarStyle;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.*;

@RequiredArgsConstructor
@Slf4j
public class LagMonitor {
    private final Admin admin;

    private final Set<String> topicNames;

    private final Set<String> consumerGroupNames;

    @Getter
    @Setter
    private boolean waitingUntilCaughtUp = true;

    @Getter
    @Setter
    private ProgressBarStyle progressBarStyle = ProgressBarStyle.ASCII;

    @Getter
    @Setter
    private int updateIntervalMillis = 330;

    @Getter
    @Setter
    private OffsetSpec offsetSpec = OffsetSpec.latest();

    private Instant lastReportAt = Instant.ofEpochMilli(0L);

    @Getter
    @Setter
    private Duration reportInterval = Duration.ofSeconds(15);

    @Getter
    @Setter
    private int asyncUpdateIntervalMs = 1900;

    public void monitor() {
        Map<TopicPartition, Long> topicPartitionOffsets = fetchOffsetsToWaitFor();

        //
        Map<GroupTopicPartition, Long> consumerOffsets = new HashMap<>();

        // fetch
        updateConsumerGroupOffsets(consumerOffsets);

        // compute total lag
        Map<GroupTopicPartition, Long> lags = new HashMap<>();
        updateLags(topicPartitionOffsets, consumerOffsets, lags);
        reportLag(lags);
        var initialTotalLag = totalLag(lags);
        if (!waitingUntilCaughtUp) {
            System.err.print("Total lag: ");
            System.err.flush();
            System.out.println(initialTotalLag);
            System.out.flush();
            return;
        }
        keepMonitoringUntilCaughtUp(topicPartitionOffsets, consumerOffsets, lags, initialTotalLag);
    }

    private Map<TopicPartition, Long> fetchOffsetsToWaitFor() {
        // Get latest message counts for each topic
        if (log.isDebugEnabled()) {
            log.debug("Describing topics: {}", String.join(",", topicNames));
        }
        var describeTopicsResult = admin.describeTopics(topicNames).values();

        Map<TopicPartition, Long> topicPartitionOffsets = processTopicDescription(describeTopicsResult);

        // Look up 10 partitions at a time
        chunks(ProgressBar.wrap(topicPartitionOffsets.keySet()
                        .stream(),
                progressBarBuilder().setTaskName("Fetch offsets").setUnit("partitions", 1)
        ), 10).forEachOrdered(tps -> {
            try {
                if (log.isDebugEnabled()) {
                    log.debug("Fetching offsets for {} (offsetSpec: {})", tps
                                    .stream()
                                    .map(TopicPartition::toString)
                                    .collect(joining(",")),
                            offsetSpec);
                }
                var offsetResult = admin.listOffsets(tps.stream().collect(toMap(e -> e, e -> offsetSpec))).all().get();
                for (var topicPartitionResult : offsetResult.entrySet()) {
                    var topicPartition = topicPartitionResult.getKey();
                    topicPartitionOffsets.put(topicPartition, topicPartitionResult.getValue().offset());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SmartProducerException("Interrupted while fetching topic partition offsets.", e);
            } catch (ExecutionException e) {
                throw new SmartProducerException("Error while fetching topic partition offsets.", e.getCause());
            }
        });
        return topicPartitionOffsets;
    }

    private Map<TopicPartition, Long> processTopicDescription(Map<String, KafkaFuture<TopicDescription>> describeTopicsResult) {
        Map<TopicPartition, Long> topicPartitionOffsets = new HashMap<>();

        for (var topicFuture : ProgressBar.wrap(describeTopicsResult.entrySet(), progressBarBuilder().setTaskName("Fetch topics").setUnit("topics", 1))) {
            try {
                var topicDescription = topicFuture.getValue().get();
                for (TopicPartitionInfo partition : topicDescription.partitions()) {
                    topicPartitionOffsets.put(new TopicPartition(topicFuture.getKey(), partition.partition()), -1L);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SmartProducerException("Interrupted while waiting for description of topic '" + topicFuture.getKey() + "'.", e);
            } catch (ExecutionException e) {
                throw new SmartProducerException("Error while waiting for description of topic '\" + topicFuture.getKey() + \"'.", e.getCause());
            }
        }
        return topicPartitionOffsets;
    }

    private void keepMonitoringUntilCaughtUp(Map<TopicPartition, Long> topicPartitionOffsets, Map<GroupTopicPartition, Long> consumerOffsets, Map<GroupTopicPartition, Long> lags, long initialTotalLag) {
        try (var bar = progressBarBuilder().setTaskName("Consume messages").setInitialMax(initialTotalLag).setUnit("msgs", 1).setUpdateIntervalMillis(asyncUpdateIntervalMs).build()) {
            long newTotalLag;
            do {
                try {
                    Thread.sleep(asyncUpdateIntervalMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("Waiting for lag to disappear interrupted. Will abort.", e);
                    break;
                }
                totalByGroup(lags).entrySet().stream().filter(e -> e.getValue() == 0).forEach(e -> {
                    log.debug("Removing group '{}' because it has a total lag of zero.", e.getKey());
                    consumerGroupNames.remove(e.getKey());
                    consumerOffsets.keySet().removeIf(g -> g.group().equals(e.getKey()));
                    lags.keySet().removeIf(g -> g.group().equals(e.getKey()));
                });

                updateConsumerGroupOffsets(consumerOffsets);
                updateLags(topicPartitionOffsets, consumerOffsets, lags);
                reportLag(lags);
                newTotalLag = totalLag(lags);
                var totalProgress = Math.max(0, initialTotalLag - newTotalLag);
                bar.stepTo(totalProgress);
            } while (newTotalLag > 0);
        }
    }

    private void updateConsumerGroupOffsets(Map<GroupTopicPartition, Long> consumerOffsets) {
        consumerGroupNames.forEach(name -> updateConsumerGroupOffsets(name, consumerOffsets));
    }

    private void updateLags(Map<TopicPartition, Long> topicPartitionOffsets, Map<GroupTopicPartition, Long> consumerOffsets, Map<GroupTopicPartition, Long> lags) {
        for (var consumerEntry : consumerOffsets.entrySet()) {
            var currentOffset = topicPartitionOffsets.get(consumerEntry.getKey().topicPartition());

            // Not all reported offsets might be of interest to us
            if(currentOffset != null) {
                lags.put(consumerEntry.getKey(), Math.max(currentOffset - consumerEntry.getValue(), 0));
            }
        }
    }

    private void reportLag(Map<GroupTopicPartition, Long> lags) {
        var now = Instant.now();
        if (Duration.ofMillis(now.toEpochMilli() - lastReportAt.toEpochMilli()).compareTo(reportInterval) < 0) {
            return;
        }

        var totalByGroup = totalByGroup(lags);

        System.out.println();
        totalByGroup.keySet().stream().sorted().forEach(group -> {
            System.out.println("GROUP: " + group + ": " + totalByGroup.get(group));
            var topics = lags.entrySet().stream().filter(e -> e.getKey().group().equals(group)).collect(groupingBy(e -> e.getKey().topic()));
            topics.keySet().stream().sorted().forEach(topic -> {
                var partitions = topics.get(topic).stream().sorted(Comparator.comparingInt(e -> e.getKey().partition())).collect(toList());
                System.out.println("  TOPIC: " + topic + ": " + partitions.stream().mapToLong(Map.Entry::getValue).sum());
                for (var partition : partitions) {
                    System.out.println("    PARTITION: " + partition.getKey().topicPartition() + ": " + partition.getValue());
                }
            });
        });
        System.out.flush();
        lastReportAt = Instant.now();
    }

    private Map<String, Long> totalByGroup(Map<GroupTopicPartition, Long> lags) {
        return lags.entrySet().stream().collect(groupingBy(e -> e.getKey().group(), summingLong(Map.Entry::getValue)));
    }

    private long totalLag(Map<GroupTopicPartition, Long> lags) {
        return lags.values().stream().mapToLong(e -> e).sum();
    }

    @Value
    static class LagEntry {
        GroupTopicPartition groupTopicPartition;
        long lag;
    }

    @EqualsAndHashCode
    @ToString
    @RequiredArgsConstructor
    static class GroupTopicPartition {
        @NonNull
        private final String group;
        @NonNull
        private final TopicPartition topicPartition;

        public TopicPartition topicPartition() {
            return topicPartition;
        }

        public String topic() {
            return topicPartition.topic();
        }

        public int partition() {
            return topicPartition.partition();
        }

        public String group() {
            return group;
        }
    }

    private void updateConsumerGroupOffsets(String consumerGroupName, Map<GroupTopicPartition, Long> consumerOffsets) {
        try {
            log.debug("Fetching group offsets for '{}'", consumerGroupName);
            var groupOffsetsResult = admin.listConsumerGroupOffsets(consumerGroupName).partitionsToOffsetAndMetadata().get();
            for (var entry : groupOffsetsResult.entrySet()) {
                log.debug("group={}, topic={}, partition={}, offset={}", consumerGroupName,
                        entry.getKey().topic(), entry.getKey().partition(), entry.getValue().offset());
                var key = new GroupTopicPartition(consumerGroupName, entry.getKey());
                consumerOffsets.put(key, entry.getValue().offset());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SmartProducerException("Interrupted while listing consumer offsets for group '"
                    + consumerGroupName + "'", e);
        } catch (ExecutionException e) {
            throw new SmartProducerException("Error while listing consumer offsets for group '"
                    + consumerGroupName + "'", e);
        }
    }

    @SuppressWarnings("SameParameterValue")
    private <T> Stream<List<T>> chunks(Stream<T> sourceStream, int size) {
        var source = sourceStream.spliterator();
        return StreamSupport.stream(new Spliterator<>() {
            final List<T> buf = new ArrayList<>();

            @Override
            public boolean tryAdvance(Consumer<? super List<T>> action) {
                while (buf.size() < size) {
                    if (!source.tryAdvance(buf::add)) {
                        if (!buf.isEmpty()) {
                            action.accept(List.copyOf(buf));
                            buf.clear();
                            return true;
                        } else {
                            return false;
                        }
                    }
                }
                action.accept(List.copyOf(buf));
                buf.clear();
                return true;
            }

            @Override
            public Spliterator<List<T>> trySplit() {
                return null;
            }

            @Override
            public long estimateSize() {
                var sourceSize = source.estimateSize();
                return sourceSize / size + (sourceSize % size != 0 ? 1 : 0);
            }

            @Override
            public int characteristics() {
                return NONNULL | ORDERED;
            }
        }, false);
    }

    private ProgressBarBuilder progressBarBuilder() {
        var pbb = new ProgressBarBuilder();
        pbb.setStyle(progressBarStyle);
        pbb.setUpdateIntervalMillis(updateIntervalMillis);
        pbb.showSpeed();
        return pbb;
    }
}
