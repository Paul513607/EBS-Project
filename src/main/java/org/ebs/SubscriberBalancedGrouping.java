package org.ebs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

public class SubscriberBalancedGrouping implements CustomStreamGrouping {
	private List<Integer> targetTasks;
	private Map<String, Integer> subscriberTaskMap;
	private int index = 0;

	@Override
	public void prepare(WorkerTopologyContext workerTopologyContext, GlobalStreamId globalStreamId, List<Integer> list) {
		this.targetTasks = new ArrayList<>(list);
		this.subscriberTaskMap = new HashMap<>();
	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		String subscriberId = (String) values.get(0);  // Assuming the subscriber ID is the first value in the tuple
		if (!subscriberTaskMap.containsKey(subscriberId)) {
			subscriberTaskMap.put(subscriberId, targetTasks.get(index));
			index = (index + 1) % targetTasks.size();
		}
		List<Integer> chosenTasks = new ArrayList<>();
		chosenTasks.add(subscriberTaskMap.get(subscriberId));
		return chosenTasks;
	}
}