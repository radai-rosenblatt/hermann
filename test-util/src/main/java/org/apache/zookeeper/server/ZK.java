package org.apache.zookeeper.server;

import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.concurrent.CountDownLatch;

public class ZK extends ZooKeeperServerShutdownHandler implements AutoCloseable {
    private final File dataDir;
    private final File snapDir;
    private final int tickMillis;
    private final InetSocketAddress bindAddress;
    private final CountDownLatch startupLatch = new CountDownLatch(1);
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    
    private ZooKeeperServer zkServer = null;
    private ServerCnxnFactory cnxnFactory = null;
    private InetSocketAddress boundAddress = null;

    public ZK() {
        this(null, null, ZooKeeperServer.DEFAULT_TICK_TIME, new InetSocketAddress("localhost", 0));
    }

    public ZK(File dataDir, File snapDir, int tickMillis, InetSocketAddress bindAddress) {
        super(null);
        try {
            if (dataDir == null) {
                dataDir = Files.createTempDirectory("zk-data").toFile();
            }
            if (snapDir == null) {
                snapDir = Files.createTempDirectory("zk-snap").toFile();
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        this.dataDir = dataDir;
        this.snapDir = snapDir;
        this.tickMillis = tickMillis;
        this.bindAddress = bindAddress;
        startup();
    }
    
    public String buildConnectionString() {
        return boundAddress.getHostName() + ":" + boundAddress.getPort();
    }
    
    public ZkClient newClient() {
        return new ZkClient(buildConnectionString());
    }
    
    @Override
    public void close() {
        if (cnxnFactory != null) {
            cnxnFactory.shutdown();
            try {
                cnxnFactory.join();
            } catch (Exception e) {
                //TODO - log?
            }
        }
        if (zkServer != null && zkServer.canShutdown()) {
            zkServer.shutdown(true);
        }
    }
    
    private void startup() {
        zkServer = new ZooKeeperServer();
        zkServer.registerServerShutdownHandler(this);
        try {
            FileTxnSnapLog txnLog = new FileTxnSnapLog(dataDir, snapDir);
            zkServer.setTxnLogFactory(txnLog);
            zkServer.setTickTime(tickMillis);
            cnxnFactory = ServerCnxnFactory.createFactory();
            cnxnFactory.configure(bindAddress, 50);
            cnxnFactory.startup(zkServer);
            boundAddress = cnxnFactory.getLocalAddress();
        } catch (Exception e) {
            startupLatch.countDown();
            shutdownLatch.countDown();
            throw new IllegalStateException("while starting up", e);
        }
    }
    
    @Override
    void handle(ZooKeeperServer.State state) {
        switch (state) {
            case INITIAL:
                break;
            case RUNNING:
                startupLatch.countDown();
                break;
            case ERROR:
                shutdownLatch.countDown();
                break;
            case SHUTDOWN:
                shutdownLatch.countDown();
                break;
            default:
                throw new IllegalStateException("unhandled: " + state);
        }
    }
}
