package net.radai.hermann.testutil;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.zookeeper.server.ZK;
import scala.Option;
import scala.collection.mutable.ArrayBuffer;

import java.io.File;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class Kafka implements AutoCloseable {
    private final static AtomicInteger BROKER_ID_GENERATOR = new AtomicInteger(0);
    private final static AtomicInteger GROUP_ID_GENERATOR = new AtomicInteger(0);
    
    private final int id;
    private final File logDir;
    private final KafkaServer broker;
    
    public Kafka(ZK zk) {
        id = BROKER_ID_GENERATOR.incrementAndGet();
        try {
            logDir = Files.createTempDirectory("kafka-data-" + id).toFile();
            Map<Object, Object> configMap = new HashMap<>();
            configMap.put("broker.id", id);
            configMap.put("zookeeper.connect", zk.buildConnectionString());
            configMap.put("log.dir", logDir.getCanonicalPath());
            configMap.put("offsets.topic.replication.factor", (short) 1);
            configMap.put("offsets.topic.num.partitions", 1);
            KafkaConfig config = new KafkaConfig(configMap);
            broker = new KafkaServer(config, Time.SYSTEM, Option.empty(), new ArrayBuffer<>());
            broker.startup();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
    
    public String getConnectionString() {
        return "localhost:" + broker.boundPort(new ListenerName(SecurityProtocol.PLAINTEXT.name()));
    }
    
    public String getId() {
        return Integer.toString(id);
    }
    
    @Override
    public void close() {
        broker.shutdown();
        broker.awaitShutdown();
    }
    
    public KafkaProducer<byte[], byte[]> newProducer() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getConnectionString());
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        return new KafkaProducer<>(props);
    }
    
    public KafkaConsumer<byte[], byte[]> newConsumer() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getConnectionString());
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getCanonicalName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getCanonicalName());
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-" + GROUP_ID_GENERATOR.incrementAndGet());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(props);
    }
    
}
