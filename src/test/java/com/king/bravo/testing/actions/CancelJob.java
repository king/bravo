package com.king.bravo.testing.actions;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;

public class CancelJob extends OneTimePipelineAction {

	@Override
	public void onceExecuteClusterAction(ClusterClient<?> client, JobID id) throws Exception {
		client.cancelWithSavepoint(id, null);
	}
}