package com.zlk;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

public class ZKClientTest {

    private static final Logger logger = Logger.getLogger(ZKClientTest.class);

    private static String zkAddress = "192.168.1.103:2181,192.168.1.104:2181,192.168.1.108:2181";
    private static int sessionTime = 2 * 1000;
    private static String path = "/zlk";

    public static ZooKeeper getInstance() throws IOException {
        return new ZooKeeper(zkAddress, sessionTime, new Watcher() {
            public void process(WatchedEvent event) {
            }
        });
    }

    public static void closeZk(ZooKeeper zooKeeper) throws InterruptedException {
        if (zooKeeper != null) {
            zooKeeper.close();
        }
    }

    public static String createZNode(ZooKeeper zk, String path, String value) throws KeeperException, InterruptedException {
        return zk.create(path, value.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public static String getData(ZooKeeper zk, String path) throws KeeperException, InterruptedException {
        return String.valueOf(zk.getData(path, false, new Stat()));
    }


    public static void main(String[] args) throws InterruptedException {
        ZKClientTest zkClientTest = null;
        ZooKeeper zk = null;
        try {
            zkClientTest = new ZKClientTest();
            if (zkClientTest == null) {
                logger.info("null");
                return;
            }
            zk = zkClientTest.getInstance();
            if (zk.exists(path, false) == null) {
                zkClientTest.createZNode(zk, path, "hello_world");
                String data = zkClientTest.getData(zk, path);
                logger.info("获取数据：" + data);
            } else {
                logger.info("已创建了该节点");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            zkClientTest.closeZk(zk);
        }
    }
}
