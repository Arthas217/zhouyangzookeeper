package com.zlk.dahai;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ZkClientTest2 {
    private static final Logger logger = Logger.getLogger(ZkClientTest2.class);

    private static ZooKeeper zkClient;
    private static String address = "192.168.1.108:2181";
    private static int sessiontime = 2 * 1000;

    public static void init() throws IOException {
        zkClient = new ZooKeeper(address, sessiontime, new Watcher() {
            public void process(WatchedEvent event) {
                // 监控节点变化
                try {
                    getChildAndWatch("/");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    /**
     * 获取子节点 && 输出所有节点
     */
    public static void getChildAndWatch(String path) throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren(path, true);
        logger.info("--------start-----------");
        for (String child : children) {
            System.out.println(child);
        }
        logger.info("-------- end------------");
    }

    /**
     * 判断节点是否存在
     */
    public static boolean isExistNode(String path) throws KeeperException, InterruptedException {
        Stat exists = zkClient.exists(path, false);
        logger.info("--------exist node: {}:" + exists);
        return exists != null;
    }

    public static void main(String[] args) {
        String pathTest = "/zlk";
        try {
            System.out.println("+++++++");
            ZkClientTest2.init();// 初始化
            System.out.println("++++++++");
            if (isExistNode(pathTest)) {
                zkClient.delete(pathTest, 0);
            }
            String path = zkClient.create(pathTest, "zlkdata".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT, new Stat());// 创建节点
            logger.info("--------------------path --------------------:" + path);
//            getChildAndWatch("/");
            TimeUnit.SECONDS.sleep(Integer.MAX_VALUE);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
