package com.zlk;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class WatchOne {

    private static final Logger logger = Logger.getLogger(WatchOne.class);

    private static String zkAddress = "192.168.1.103:2181,192.168.1.104:2181,192.168.1.108:2181";
    private static int sessionTime = 2 * 1000;
    private static String PATH = "/zlk1";
    private ZooKeeper zooKeeper;

    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    public void setZooKeeper(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }


    public ZooKeeper getInstance() throws IOException {
        return new ZooKeeper(zkAddress, sessionTime, new Watcher() {
            public void process(WatchedEvent event) {

            }
        });
    }

    public void closeZk() throws InterruptedException {
        if (zooKeeper != null) {
            zooKeeper.close();
        }
    }

    public String createZNode(String path, String value) throws KeeperException, InterruptedException {
        return zooKeeper.create(path, value.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public String getZnodeData(final String path) throws KeeperException, InterruptedException {
        String result = null;
        //  watcher埋雷
        byte[] bytes = zooKeeper.getData(path, new Watcher() {
            public void process(WatchedEvent event) {
                try {
                    // 触发获取更新的新值
                    trigerValue(path);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, new Stat());
        result = new String(bytes);
        return result;
    }

    private String trigerValue(String path) throws KeeperException, InterruptedException {
        byte[] bytes = zooKeeper.getData(PATH, false, new Stat());
        String result = new String(bytes);
        logger.info("--------------------watch one time :" + result);
        return result;
    }


    public static void main(String[] args) throws InterruptedException, KeeperException, IOException {
        WatchOne watchOne = new WatchOne();

        watchOne.setZooKeeper(watchOne.getInstance());
        if (watchOne.getZooKeeper().exists(PATH, false) == null) {
            watchOne.createZNode(PATH, "AAA");
            String retValue = watchOne.getZnodeData(PATH);
            logger.info("---------------------retValue :" + retValue);
            // 模拟不关闭zk
            TimeUnit.SECONDS.sleep(Long.MAX_VALUE);
        } else {
            logger.info("---------------------node ok");
        }
    }
}
