package wirecard.fot;

import kafka.tools.StreamsResetter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.stream.Collectors;

import static wirecard.fot.ApplicationProps.*;

public class OffsetSetter {
    private static Logger logger = LoggerFactory.getLogger("ConsumerOffsetSetter");

    public static void setOffsets() {

        Map<TopicPartition, Long> partitionStartingPoint = extractSourcePartitionsOffset();
        pointTargetConsumerOffsets(partitionStartingPoint);

    }

    private static void pointTargetConsumerOffsets(Map<TopicPartition, Long> partitionStartingPoint) {
        /** Assigning the Partitions to the target Consumer */
        Consumer targetConsumer = createTargetConsumer();
        targetConsumer.assign(partitionStartingPoint.keySet());
        logger.info("Assigned partitions to target Consumer ... ");

        /** Calling Kafka Reset Tool to apply new offset points to the target consumer */
        StreamsResetter streamsResetter = new StreamsResetter();
        streamsResetter.resetOffsetsFromResetPlan(targetConsumer, partitionStartingPoint.keySet(), partitionStartingPoint);
        logger.info("Assigned partitions offset to target Consumer ... ");
        targetConsumer.close(Duration.ofSeconds(5));
    }

    private static Map<TopicPartition, Long> extractSourcePartitionsOffset() {
        Consumer consumer = createSourceConsumer();
        List<TopicPartition> partitions = extractAllPartitions(consumer, sourceTopic);

        return extractCurrentConsumerPosition(consumer, partitions);
    }

    private static Map<TopicPartition, Long> extractCurrentConsumerPosition(Consumer consumer, List<TopicPartition> partitions) {
        logger.info("Extracting current offset positions from source consumer of the topic {} ", sourceTopic);
        consumer.assign(partitions);
        Map<TopicPartition, Long> partitionOffsetMap = new HashMap<>();
        partitions.stream().forEach(
                partition -> partitionOffsetMap.put(partition, consumer.position(partition))
        );
        consumer.close(Duration.ofSeconds(2));
        logger.info("Found Partitions/Positions {}", partitionOffsetMap.entrySet().stream()
                .map(
                        p -> new SimpleEntry<>(p.getKey().partition(), p.getValue())
                ).collect(Collectors.toList()));
        return partitionOffsetMap;
    }

    private static List<TopicPartition> extractAllPartitions(Consumer consumer, String sourceTopic) {
        logger.info("Extracting available partitions from source consumer of the topic {} ", sourceTopic);
        List<PartitionInfo> infos = consumer.partitionsFor(sourceTopic);
        List<TopicPartition> partitions = new ArrayList<>();
        infos.stream().forEach(
                info -> partitions.add(new TopicPartition(sourceTopic, info.partition()))
        );
        logger.info("Found Partitions: {}", partitions.stream().map(p -> p.partition()).collect(Collectors.toList()));
        return partitions;
    }

    private static Consumer createSourceConsumer() {
        return createConsumer(sourceApplicationId);
    }

    private static Consumer createTargetConsumer() {
        return createConsumer(fotAppId);
    }

    private static Consumer createConsumer(String applicationId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, applicationId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, fotClientId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        return consumer;
    }
}
