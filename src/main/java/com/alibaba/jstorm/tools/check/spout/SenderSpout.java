package com.alibaba.jstorm.tools.check.spout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.cluster.Cluster;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.schedule.Assignment;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.task.error.TaskReportError;
import com.alibaba.jstorm.tools.check.HelloTuple;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.NetWorkUtils;

import backtype.storm.generated.Grouping;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class SenderSpout implements IRichSpout {
	private static final Logger LOG = LoggerFactory.getLogger(SenderSpout.class);
	private static final long serialVersionUID = 1L;
	SpoutOutputCollector collector;
	private static final int EMIT_BATCH_NUM = 1000;
	private static final int ERROR_THREADHOLD_COUNTER = 100;
	private static final String MSG_SPLITTER = "@";
	private int taskId;
	private int taskIndex;
	private List<Integer> targetTasks;
	private int timeout;
	private String topologyId;
	private StormClusterState zkClusterState;
	private TaskReportError zkReporter;
	private String host;
	private Map<String, AtomicInteger> metrix;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void getTargetTasks(TopologyContext context) {
		Map<String, Map<String, Grouping>> targets = context.getThisTargets();

		Set<String> receivers = ((Map) targets.get("default")).keySet();

		this.targetTasks = new ArrayList();
		for (String receiver : receivers) {
			List<Integer> tasks = context.getComponentTasks(receiver);
			this.targetTasks.addAll(tasks);
		}
		Collections.sort(this.targetTasks);
	}

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;

		this.metrix = new ConcurrentHashMap<>();

		this.taskId = context.getThisTaskId();
		this.taskIndex = context.getThisTaskIndex();
		this.topologyId = context.getTopologyId();
		this.timeout = JStormUtils.parseInt(conf.get("topology.message.timeout.secs"), 30).intValue();

		getTargetTasks(context);

		this.host = NetWorkUtils.hostname();
		try {
			this.zkClusterState = Cluster.mk_storm_cluster_state(conf);
		} catch (Exception e) {
			LOG.error("Failed to create zk Connection", e);
		}
		this.zkReporter = new TaskReportError(this.zkClusterState, this.topologyId, this.taskId);
		LOG.info("Finish open");
	}

	private void initMetrix(Assignment assignment, Map<Integer, String> taskId2Host) {
		Map<String, String> id2host = assignment.getNodeHost();
		for (Integer taskId : this.targetTasks) {
			ResourceWorkerSlot resource = assignment.getWorkerByTaskId(taskId);
			if (resource == null) {
				LOG.warn("No worker of task " + taskId);
			} else {
				String host = (String) id2host.get(resource.getNodeId());
				if (host == null) {
					LOG.warn("No hostname of task:{}, supervisorId:{}", taskId, resource.getNodeId());
				} else {
					this.metrix.put(host, new AtomicInteger());
					taskId2Host.put(taskId, host);
				}
			}
		}
	}

	@Override
	public void nextTuple() {
		this.metrix.clear();
		Assignment assignment;
		try {
			assignment = this.zkClusterState.assignment_info(this.topologyId, null);
		} catch (Exception e) {
			LOG.error("Failed to get " + this.topologyId + " Assignment", e);
			throw new RuntimeException();
		}
		Map<Integer, String> taskId2Host = new HashMap<>();

		initMetrix(assignment, taskId2Host);
		for (int index = 0; index < this.targetTasks.size(); index++) {
			int task = ((Integer) this.targetTasks.get((index + this.taskIndex) % this.targetTasks.size())).intValue();
			String targetHost = (String) taskId2Host.get(Integer.valueOf(task));
			if (targetHost == null) {
				LOG.warn("Task no hostname {}", Integer.valueOf(task));
			} else {
				for (int i = 0; i < EMIT_BATCH_NUM; i++) {
					String msgId = targetHost + MSG_SPLITTER + i;

					HelloTuple tuple = new HelloTuple(this.host, msgId, System.currentTimeMillis());

					this.collector.emitDirect(task, new Values(new Object[] { tuple }), msgId);
				}
				JStormUtils.sleepMs(1000L);
			}
		}
		JStormUtils.sleepMs(4 * this.timeout * 1000);
		for (Map.Entry<String, AtomicInteger> entry : this.metrix.entrySet()) {
			AtomicInteger counter = (AtomicInteger) entry.getValue();
			if (counter.get() > ERROR_THREADHOLD_COUNTER) {
				StringBuilder sb = new StringBuilder();

				sb.append("Connection between ").append(this.host).append(" and ").append((String) entry.getKey())
						.append(" is bad ");

				LOG.warn(sb.toString());

				this.zkReporter.report(new RuntimeException(sb.toString()));
			}
		}
	}

	@Override
	public void close() {
		LOG.info("Close {}:{}", this.topologyId, this.taskId);
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
		String msgId = (String) id;

		String[] msgItems = msgId.split(MSG_SPLITTER);
		if ((msgItems == null) || (msgItems.length == 0)) {
			LOG.warn("{} bad format", msgId);
			return;
		}
		String targetHost = msgItems[0];

		AtomicInteger counter = (AtomicInteger) this.metrix.get(targetHost);
		if (counter == null) {
			LOG.error("No counter of {}", targetHost);
			return;
		}
		counter.incrementAndGet();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(new String[] { "HelloTuple" }));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public void activate() {
		LOG.info("Start active");
	}

	@Override
	public void deactivate() {
		LOG.info("Start deactive");
	}

	public boolean isDistributed() {
		return true;
	}
}
