package com.king.bravo.testing.actions;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;

public class TriggerFailure extends OneTimePipelineAction {

	@Override
	public void onceExecuteClusterAction(ClusterClient<?> client, JobID id) throws Exception {
		Thread.sleep(1000);
		throw new RuntimeException("Triggered failure");
	}
}