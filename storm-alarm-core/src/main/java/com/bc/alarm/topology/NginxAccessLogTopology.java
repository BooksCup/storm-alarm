package com.bc.alarm.topology;

import com.bc.alarm.bolt.NginxAccessLogBolt;
import com.bc.alarm.scheme.MessageScheme;
import kafka.api.OffsetRequest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * NginxAccessLog拓扑
 *
 * @author zhou
 */
public class NginxAccessLogTopology {
    /**
     * 日志
     */
    private static final Log logger = LogFactory.getLog(NginxAccessLogTopology.class);


    public static void main(String[] args) throws Exception {
        logger.info("=====context start======");

        // config
        String topic = "nginx_access_log";
        String zkRoot = "/brokers";
        String spoutId = "KafkaSpout";
        String brokerZkStr = "192.168.0.2:2181";
        int zkPort = 2181;
        String[] zkServers = new String[]{"192.168.0.2"};


        BrokerHosts hosts = new ZkHosts(brokerZkStr);
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, spoutId);

        spoutConfig.ignoreZkOffsets = false;
        spoutConfig.startOffsetTime = OffsetRequest.LatestTime();

        spoutConfig.zkPort = zkPort;
        List<String> servers = new ArrayList<>();
        for (String zkServer : zkServers) {
            servers.add(zkServer);
        }
        spoutConfig.zkServers = servers;

        spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(spoutId, new KafkaSpout(spoutConfig));
        builder.setBolt("nginxAccessLogBolt", new NginxAccessLogBolt()).shuffleGrouping(spoutId);

        Config config = new Config();
        // 这个设置一个spout task上面最多有多少个没有处理(ack/fail)的tuple，防止tuple队列过大, 只对可靠任务起作用
        config.setMaxSpoutPending(100000);
        // 默认是30s
        config.setMessageTimeoutSecs(1000);

        config.setDebug(true);

        if (args != null && args.length > 0) {
            config.setNumWorkers(10);
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", config, builder.createTopology());
        }
    }
}
