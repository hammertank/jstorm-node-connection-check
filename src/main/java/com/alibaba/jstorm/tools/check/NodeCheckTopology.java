package com.alibaba.jstorm.tools.check;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.tools.check.bolt.ReceiverBolt;
import com.alibaba.jstorm.tools.check.spout.SenderSpout;

public class NodeCheckTopology {
	// private static final String TOPOLOGY_SPOUT_PARALLELISM_HINT =
	// "topology_spout_parallelism_hint";
	// private static final String TOPOLOGY_BOLT_PARALLELISM_HINT =
	// "topology_bolt_parallelism_hint";
	private static final String TOPOLOGY_NAME = "check.node.connection";
	private static final String SPOUT_NAME = "sender";
	private static final String BOLT_NAME = "bolt";

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void SetBuilder(TopologyBuilder builder, Map conf,
			Integer nodeNum) {
		Map componentMap = new HashMap();

		ConfigExtension.setTaskOnDifferentNode(componentMap, true);

		builder.setSpout(SPOUT_NAME, new SenderSpout(), nodeNum)
				.addConfigurations(componentMap);

		builder.setBolt(BOLT_NAME, new ReceiverBolt(), nodeNum)
				.addConfigurations(componentMap).shuffleGrouping(SPOUT_NAME);

		Config.setNumAckers(conf, 2);

		conf.put(Config.TOPOLOGY_WORKERS, nodeNum);

		// conf.put(Config.STORM_CLUSTER_MODE, "distributed");
		// conf.put("storm.messaging.transport",
		// "com.alibaba.jstorm.message.zeroMq.MQContext");
	}

	@SuppressWarnings("rawtypes")
	public static TopologyBuilder SetRemoteTopology(Map conf, Integer nodeNum)
			throws Exception {
		TopologyBuilder builder = new TopologyBuilder();

		SetBuilder(builder, conf, nodeNum);

		return builder;
	}

	@SuppressWarnings("rawtypes")
	public static TopologyBuilder SetLocalTopology(Map conf) {
		TopologyBuilder builder = new TopologyBuilder();

		SetBuilder(builder, conf, 1);

		return builder;
	}

	@SuppressWarnings("rawtypes")
	public static void main(String[] args) throws Exception {

		Map conf = new HashMap();

		if (args.length == 0) {
			// System.err.println("Invalid parameter, please input node
			// number");
			// System.exit(-1);
			LocalCluster cluster = new LocalCluster();
			TopologyBuilder builder = SetLocalTopology(conf);
			cluster.submitTopology(TOPOLOGY_NAME, conf,
					builder.createTopology());
			// Thread.sleep(30000);
			cluster.shutdown();

		} else {
			Integer nodeNum = Integer.valueOf(Integer.parseInt(args[0]));
			if (nodeNum < 0 || nodeNum >= 32767) {
				System.err
						.println("Invalid nodeNum parameter, please input correct node number"
								+ args[0]);
				System.exit(-1);
			}
			TopologyBuilder builder = SetRemoteTopology(conf, nodeNum);
			StormSubmitter.submitTopology(TOPOLOGY_NAME, conf,
					builder.createTopology());
		}
	}
}
