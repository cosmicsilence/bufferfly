package io.bufferfly.spring.benchmark;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * The ledger topic shared by the ledger benchmarks: one topic with
 * {@code partitions} partitions. Records are keyed by account, so Kafka's default
 * partitioner decides each account's partition.
 */
public final class LedgerTopic {

    private LedgerTopic() {
    }

    public static void create(String bootstrap, String topic, int partitions) throws Exception {
        try (Admin admin = admin(bootstrap)) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get();
        }
    }

    /** @return messages per partition (end offset of each partition), in partition order. */
    public static List<Long> partitionSizes(String bootstrap, String topic, int partitions) throws Exception {
        try (Admin admin = admin(bootstrap)) {
            Map<TopicPartition, OffsetSpec> request = new HashMap<>();
            for (int p = 0; p < partitions; p++) {
                request.put(new TopicPartition(topic, p), OffsetSpec.latest());
            }
            var offsets = admin.listOffsets(request).all().get();
            return IntStream.range(0, partitions)
                    .mapToObj(p -> offsets.get(new TopicPartition(topic, p)).offset())
                    .toList();
        }
    }

    private static Admin admin(String bootstrap) {
        return Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap));
    }
}
