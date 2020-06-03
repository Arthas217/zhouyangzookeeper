package com.zlk.dahai;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DistributeClient {

    private static final Logger logger = Logger.getLogger(DistributeClient.class);

    private String parentNode = "/servers";

    public static void main(String[] args) {
        DistributeClient distributeClient = new DistributeClient();
        try {
            //获取zk集群连接
            distributeClient.init();
            //注册监听
            distributeClient.getChildNode();
            //业务逻辑处理
            distributeClient.business();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void business() throws InterruptedException {
        TimeUnit.SECONDS.sleep(Integer.MAX_VALUE);
    }

    private void getChildNode() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren(parentNode, true);
        logger.info("--------start-----------");
        for (String child : children) {
            //注意第一次运行时可以没有parentNode这个目录 需要创建一个
            byte[] data = zkClient.getData(parentNode + "/" + child, false, new Stat());
            System.out.println(new String(data));
        }
        logger.info("-------- end------------");
    }

    private static ZooKeeper zkClient;
    private static String address = "192.168.1.108:2181,192.168.1.103:2181,192.168.1.104:2181";
    private static int sessiontime = 2 * 1000;

    private void init() throws IOException {
        zkClient = new ZooKeeper(address, sessiontime, new Watcher() {
            public void process(WatchedEvent event) {
                try {
                    //监听多次
                    getChildNode();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
