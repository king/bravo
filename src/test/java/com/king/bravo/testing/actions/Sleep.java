package com.king.bravo.testing.actions;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;

import com.king.bravo.testing.PipelineAction;

public class Sleep implements PipelineAction {

	long sleepTime;

	public Sleep(long sleepTime) {
		this.sleepTime = sleepTime;
	}

	@Override
	public void executeClusterAction(ClusterClient<?> client, JobID id) throws Exception {
		Thread.sleep(sleepTime);
	}
}