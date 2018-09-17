package com.king.bravo.testing.actions;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;

import com.king.bravo.testing.PipelineAction;

public abstract class OneTimePipelineAction implements PipelineAction {

	private boolean checkpointLockTriggered = false;
	private boolean clusterActionTriggered = false;

	public final void withCheckpointLock(SourceContext<String> ctx) {
		if (checkpointLockTriggered) {
			return;
		}

		checkpointLockTriggered = true;
		onceWithCheckpointLock(ctx);
	}

	protected void onceWithCheckpointLock(SourceContext<String> ctx) {}

	public final void executeClusterAction(ClusterClient<?> client, JobID id) throws Exception {

		if (clusterActionTriggered) {
			return;
		}

		clusterActionTriggered = true;
		onceExecuteClusterAction(client, id);
	};

	protected void onceExecuteClusterAction(ClusterClient<?> client, JobID id) throws Exception {}

}
