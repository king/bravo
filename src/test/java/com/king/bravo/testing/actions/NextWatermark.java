package com.king.bravo.testing.actions;

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.api.watermark.Watermark;

import com.king.bravo.testing.PipelineAction;

public class NextWatermark implements PipelineAction {
	long watermark;

	public NextWatermark(long watermark) {
		this.watermark = watermark;
	}

	@Override
	public void withCheckpointLock(SourceContext<String> ctx) throws Exception {
		ctx.emitWatermark(new Watermark(watermark));
		Thread.sleep(500);
	}
}