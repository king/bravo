package com.king.bravo.testing.actions;

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;

import com.king.bravo.testing.PipelineAction;

public class Process implements PipelineAction {

	final String output;
	final long timestamp;

	public Process(String output, long timestamp) {
		this.output = output;
		this.timestamp = timestamp;
	}

	@Override
	public void withCheckpointLock(SourceContext<String> ctx) throws Exception {
		ctx.collectWithTimestamp(output, timestamp);
	}
}