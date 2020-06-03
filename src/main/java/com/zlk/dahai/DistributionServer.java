package com.zlk.dahai;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * 服务端注册zk
 */
public class DistributionServer {

    public static void main(String[] args) {
        DistributionServer distributionServer = new DistributionServer();
        try {
            // 连接zk
            distributionServer.init();
            // 注册节点服务  (注意运行时，需要在运行环境的program arguments选项里 添加值
            distributionServer.register(args[0]);
            // 业务逻辑
            distributionServer.business();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void business() throws InterruptedException {
        TimeUnit.SECONDS.sleep(Integer.MAX_VALUE);
    }

    private void register(String hostname) throws KeeperException, InterruptedException {
        zkClient.create("/servers/user", hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL, new Stat());
    }


    private static ZooKeeper zkClient;
    private static String address = "192.168.1.108:2181,192.168.1.103:2181,192.168.1.408:2181";
    private static int sessiontime = 2 * 1000;

    private void init() throws IOException {
        zkClient = new ZooKeeper(address, sessiontime, new Watcher() {
            public void process(WatchedEvent event) {
            }
        });
    }
}
