package com.king.bravo;

import java.util.Random;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class InfiniteTestSource implements SourceFunction<Integer> {

	private static final long serialVersionUID = 1L;
	private volatile boolean running = true;
	private Random rnd = new Random();

	@Override
	public void run(SourceContext<Integer> ctx) throws Exception {
		while (running) {
			synchronized (ctx.getCheckpointLock()) {
				ctx.collect(rnd.nextInt(5));
			}
			Thread.sleep(500);
		}
	}

	@Override
	public void cancel() {
		running = false;
	}
}