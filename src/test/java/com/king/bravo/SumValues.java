package com.king.bravo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class SumValues implements
		MapFunction<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, Tuple2<Integer, Integer>> {
	private static final long serialVersionUID = 1L;

	@Override
	public Tuple2<Integer, Integer> map(Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> t)
			throws Exception {
		return Tuple2.of(t.f0.f0, t.f0.f1 + t.f1.f1);
	}
}