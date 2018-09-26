package net.radai.hermann.testutil;

import org.apache.zookeeper.server.ZK;
import org.junit.After;
import org.junit.Before;

public class AbstractIntegrationTest {
    protected ZK zk;
    protected Kafka kafka;
    
    @Before
    public void startup() {
        zk = new ZK();
        kafka = new Kafka(zk);
    }
    
    @After
    public void shutdown() {
        kafka.close();
        zk.close();
    }
}
