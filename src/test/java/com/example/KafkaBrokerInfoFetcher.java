package com.example;

import org.apache.zookeeper.ZooKeeper;

import java.util.List;

/**
 * Created by amarendra on 18/3/17.
 */
public class KafkaBrokerInfoFetcher {

    public static void main(String[] args) throws Exception {
        ZooKeeper zk = new ZooKeeper("localhost:2181", 10000, null);
        List<String> ids = zk.getChildren("/brokers/ids", false);
        for (String id : ids) {
            String brokerInfo = new String(zk.getData("/brokers/ids/" + id, false, null));
            System.out.println(id + ": " + brokerInfo);
        }
    }
}
