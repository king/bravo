package com.king.bravo;

import java.util.Optional;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

public class Counter extends RichMapFunction<Integer, Tuple2<Integer, Integer>> {

	private static final long serialVersionUID = 7317800376639115920L;
	private ValueState<Integer> count;

	@Override
	public void open(Configuration parameters) throws Exception {
		count = getRuntimeContext().getState(new ValueStateDescriptor<>("Count", Integer.class));
	}

	@Override
	public Tuple2<Integer, Integer> map(Integer value) throws Exception {
		count.update(Optional.ofNullable(count.value()).orElse(0) + 1);
		return Tuple2.of(value, count.value());
	}
}