package com.alibaba.jstorm.tools.check.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.jstorm.tools.check.HelloTuple;
import com.alibaba.jstorm.utils.RunCounter;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReceiverBolt implements IRichBolt {
	private static final long serialVersionUID = 1L;
	public static Logger LOG = LoggerFactory.getLogger(ReceiverBolt.class);
	private OutputCollector collector;
	private RunCounter tpsCounter;

	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.tpsCounter = new RunCounter(ReceiverBolt.class.getSimpleName());

		this.collector = collector;

		LOG.info("Finished preparation");
	}

	public void execute(Tuple input) {
		HelloTuple tuple = (HelloTuple) input.getValue(0);

		this.collector.ack(input);

		long now = System.currentTimeMillis();
		long spend = now - tuple.getEmitTs().longValue();

		this.tpsCounter.count(spend);
	}

	public void cleanup() {
		this.tpsCounter.cleanup();
		LOG.info("Finish cleanup");
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
