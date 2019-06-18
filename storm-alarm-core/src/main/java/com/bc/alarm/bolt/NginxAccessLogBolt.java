package com.bc.alarm.bolt;


import com.alibaba.fastjson.JSONObject;
import com.bc.alarm.cons.Constant;
import com.bc.alarm.entity.NginxAccessLog;
import com.bc.alarm.utils.MongoDBUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * @author zhou
 */
public class NginxAccessLogBolt extends BaseRichBolt {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     * 日志
     */
    private static final Log logger = LogFactory.getLog(NginxAccessLogBolt.class);

    OutputCollector collector;

    @Override
    public void execute(Tuple tuple) {
        try {
            String singleLog = tuple.getString(0);
            logger.info("singleLog: " + singleLog);

            NginxAccessLog nginxAccessLog = JSONObject.parseObject(singleLog, NginxAccessLog.class);
            logger.info("nginxAccessLog: " + nginxAccessLog);

            MongoDBUtils.getInstance().insertOne(Constant.COLLECTION_NAME_NGINX_ACCESS_LOG, nginxAccessLog);

            // 确认成功处理一个tuple
            collector.ack(tuple);
        } catch (Exception e) {
            logger.error("bolt handle error, [errorMsg]:" + e.getMessage());
            collector.fail(tuple);
        }
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("msg"));
    }
}
