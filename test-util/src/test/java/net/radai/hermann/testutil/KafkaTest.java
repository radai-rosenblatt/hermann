package net.radai.hermann.testutil;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.zookeeper.server.ZK;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Future;

public class KafkaTest {
    
    @Test
    public void testSimpleScenario() throws Exception {
        String topicName = "topic" + UUID.randomUUID();
        try (ZK zk = new ZK(); Kafka broker = new Kafka(zk)) {
            RecordMetadata md;
            try (KafkaProducer<byte[], byte[]> producer = broker.newProducer()) {
                Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topicName, new byte[]{1, 2, 3}, new byte[]{4, 5, 6}));
                producer.flush();
                md = future.get();
            }
            Assert.assertNotNull(md);
            try (KafkaConsumer<byte[], byte[]> consumer = broker.newConsumer()) {
                consumer.subscribe(Collections.singletonList(topicName));
                List<ConsumerRecord<byte[], byte[]>> records = poll(consumer, 1, 10000);
                Assert.assertEquals(1, records.size());
            }
        }
    }
    
    public static List<ConsumerRecord<byte[], byte[]>> poll(KafkaConsumer<byte[], byte[]> consumer, int howMany, long timeoutMs) {
        List<ConsumerRecord<byte[], byte[]>> results = new ArrayList<>();
        long now = System.currentTimeMillis();
        long deadline = now + timeoutMs;
        while (now < deadline) {
            ConsumerRecords<byte[], byte[]> poll = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<byte[], byte[]> record : poll) {
                results.add(record);
            }
            if (results.size() >= howMany) {
                break;
            }
            now = System.currentTimeMillis();
        }
        return results;
    }
}
