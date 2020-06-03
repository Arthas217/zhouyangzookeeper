package com.zlk;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class WatchMore {

    private static final Logger logger = Logger.getLogger(WatchMore.class);

    private static String zkAddress = "192.168.1.103:2181";
    private static int sessionTime = 2 * 1000;
    private static String PATH = "/zlk";
    private ZooKeeper zooKeeper;
    private String oldValue;

    public String getOldValue() {
        return oldValue;
    }

    public void setOldValue(String oldValue) {
        this.oldValue = oldValue;
    }

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
        String result;
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
        oldValue = result;
        return result;
    }

    private String trigerValue(final String nodePath) throws KeeperException, InterruptedException {
        //支持连续触发watch 埋雷
        byte[] bytes = zooKeeper.getData(PATH, new Watcher() {
            public void process(WatchedEvent event) {
                try {
                    trigerValue(nodePath);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, new Stat());
        String result = new String(bytes);
        String newValue = result;
        if (oldValue.equals(newValue)) {
            logger.info("-------------------no changes");
        } else {
            logger.info("------------------old value: " + oldValue + "\t new value " + newValue);
            oldValue = newValue;
        }
        return result;
    }


    public static void main(String[] args) throws InterruptedException, KeeperException, IOException {
        WatchMore watchMore = new WatchMore();

        watchMore.setZooKeeper(watchMore.getInstance());
        if (watchMore.getZooKeeper().exists(PATH, false) == null) {
            watchMore.createZNode(PATH, "AAA");
            String retValue = watchMore.getZnodeData(PATH);
            logger.info("---------------------retValue :" + retValue);
            // 模拟不关闭zk
            TimeUnit.SECONDS.sleep(Long.MAX_VALUE);
        } else {
            logger.info("---------------------node ok");
        }
    }
}
