package com.king.bravo;

import java.util.Optional;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

public class Counter extends RichMapFunction<Integer, Integer> {

	private static final long serialVersionUID = 7317800376639115920L;
	private ValueState<Integer> count;
	private ValueState<Integer> count2;

	@Override
	public void open(Configuration parameters) throws Exception {
		count = getRuntimeContext().getState(new ValueStateDescriptor<>("Count", Integer.class));
		count2 = getRuntimeContext().getState(new ValueStateDescriptor<>("Count2", Integer.class));
	}

	@Override
	public Integer map(Integer value) throws Exception {
		count.update(Optional.ofNullable(count.value()).orElse(0) + 1);
		count2.update(Optional.ofNullable(count2.value()).orElse(0) + 1);
		System.err.println("C1: " + value + " - " + count.value());
		System.err.println("C2: " + value + " - " + count2.value());
		return value;
	}
}