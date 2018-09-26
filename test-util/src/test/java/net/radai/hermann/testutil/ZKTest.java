package net.radai.hermann.testutil;

import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.server.ZK;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

public class ZKTest {
    
    @Test
    public void testSimpleScenario() {
        try (ZK zk = new ZK()) {
            ZkClient client = zk.newClient();
            String path = "/path" + UUID.randomUUID();
            Assert.assertFalse(client.exists(path));
            client.createEphemeral(path);
            Assert.assertTrue(client.exists(path));
            client.close();
        }
    }
}
