package com.king.bravo.testing.actions;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;

public class TriggerSavepoint extends OneTimePipelineAction {

	public static String lastSavepointPath;

	@Override
	public void onceExecuteClusterAction(ClusterClient<?> client, JobID id) throws Exception {
		lastSavepointPath = client.triggerSavepoint(id, null).get();
	}
}