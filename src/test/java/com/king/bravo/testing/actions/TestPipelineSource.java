package com.king.bravo.testing.actions;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import com.king.bravo.testing.BravoTestPipeline;
import com.king.bravo.testing.PipelineAction;

public class TestPipelineSource implements SourceFunction<String>, ListCheckpointed<Serializable> {

	private static final long serialVersionUID = 1L;

	private static long DEFAULT_SLEEP = 100;

	private volatile boolean isRunning = false;
	private int index = 0;

	@Override
	public void run(SourceContext<String> ctx) throws Exception {

		isRunning = true;

		while (index < BravoTestPipeline.actions.size()) {
			if (!isRunning) {
				return;
			}

			PipelineAction nextAction = BravoTestPipeline.actions.get(index);
			synchronized (ctx.getCheckpointLock()) {
				index++;
				nextAction.withCheckpointLock(ctx);
			}
			nextAction.executeClusterAction(BravoTestPipeline.client, BravoTestPipeline.jobID);
			Thread.sleep(DEFAULT_SLEEP);
		}

		Thread.sleep(DEFAULT_SLEEP);
		ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
	}

	@Override
	public void cancel() {
		isRunning = false;
	}

	@Override
	public void restoreState(List<Serializable> s) throws Exception {
		index = (int) s.get(0);
	}

	@Override
	public List<Serializable> snapshotState(long arg0, long arg1) throws Exception {
		return Collections.singletonList(index);
	}

}