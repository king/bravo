package com.king.bravo.testing;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import com.google.common.collect.Lists;

public class CollectingSink implements SinkFunction<String>, ListCheckpointed<Integer> {
	private static final long serialVersionUID = 1L;

	public static List<String> OUTPUT = new ArrayList<>();

	public CollectingSink() {}

	@Override
	public void invoke(String out) throws Exception {
		OUTPUT.add(out);
	}

	@Override
	public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
		return Lists.newArrayList(OUTPUT.size());
	}

	@Override
	public void restoreState(List<Integer> state) throws Exception {
		int lastValid = state.get(0);
		List<String> newOutput = new ArrayList<>();
		for (int i = 0; i < lastValid; i++) {
			newOutput.add(OUTPUT.get(i));
		}
		OUTPUT = (List<String>) newOutput;
	}

}