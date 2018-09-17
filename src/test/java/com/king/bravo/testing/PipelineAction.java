package com.king.bravo.testing;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;

public interface PipelineAction {

	default void withCheckpointLock(SourceContext<String> ctx)
			throws Exception {};

	default void executeClusterAction(ClusterClient<?> client, JobID id) throws Exception {}

}