/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.king.bravo;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.king.bravo.reader.KeyedStateReader;
import com.king.bravo.reader.OperatorStateReader;
import com.king.bravo.testing.BravoTestPipeline;

public class TtlStateTest extends BravoTestPipeline {

	private static final long serialVersionUID = 1L;

	@Test
	public void test() throws Exception {
		process("1,1");
		process("2,3");
		process("1,2");
		process("1,1");
		sleep(1000);
		cancelJob();
		runTestPipeline(this::constructTestPipeline);
		validateCheckpointedStateReading();
	}

	@SuppressWarnings("unchecked")
	private void validateCheckpointedStateReading() throws IOException, Exception {
		ExecutionEnvironment environment = ExecutionEnvironment.createLocalEnvironment();
		Savepoint savepoint = getLastCheckpoint();
		OperatorStateReader reader = new OperatorStateReader(environment, savepoint, "hello");

		List<Tuple3<Integer, String, Integer>> countState = reader
				.readKeyedStates(KeyedStateReader.forMapStateEntries("Map", BasicTypeInfo.INT_TYPE_INFO,
						BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO))
				.collect();

		List<Integer> mapValues = reader
				.readKeyedStates(KeyedStateReader.forMapStateValues("Map", BasicTypeInfo.INT_TYPE_INFO, true))
				.collect();

		Collections.sort(mapValues);
		assertEquals(Lists.newArrayList(1, 1, 2), mapValues);

		assertEquals(Sets.newHashSet(Tuple3.of(1, "1", 2), Tuple3.of(1, "2", 1), Tuple3.of(2, "3", 1)),
				new HashSet<>(countState));

		List<Tuple2<Integer, List<Integer>>> listState = reader.readKeyedStates(
				KeyedStateReader.forListStates("List", BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO))
				.collect();

		assertEquals(Sets.newHashSet(Tuple2.of(1, Lists.newArrayList(1, 2, 1)), Tuple2.of(2, Lists.newArrayList(3))),
				new HashSet<>(listState));

		List<Tuple2<Integer, Integer>> listStateValues = reader.readKeyedStates(
				KeyedStateReader.forListStateValues("List", BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO))
				.collect();

		assertEquals(Sets.newHashSet(Tuple2.of(1, 1), Tuple2.of(1, 2), Tuple2.of(2, 3)),
				new HashSet<>(listStateValues));

		List<Tuple2<Integer, Integer>> valuePairs = reader.readKeyedStates(
				KeyedStateReader.forValueStateKVPairs("Val", BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO))
				.collect();

		assertEquals(Sets.newHashSet(Tuple2.of(1, 1), Tuple2.of(2, 3)), new HashSet<>(valuePairs));

		List<Integer> values = reader.readKeyedStates(
				KeyedStateReader.forValueStateValues("Val", BasicTypeInfo.INT_TYPE_INFO))
				.collect();

		assertEquals(Sets.newHashSet(1, 3), new HashSet<>(values));

	}

	public DataStream<String> constructTestPipeline(DataStream<String> source) {
		return source
				.map(s -> {
					String[] split = s.split(",");
					return Tuple2.of(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
				})
				.returns(new TypeHint<Tuple2<Integer, Integer>>() {})
				.keyBy(0)
				.map(new MapCounter())
				.uid("hello");
	}

	public static class MapCounter extends RichMapFunction<Tuple2<Integer, Integer>, String> {

		private static final long serialVersionUID = 7317800376639115920L;
		private MapState<String, Integer> mapState;
		private ListState<Integer> listState;
		private ValueState<Integer> valueState;

		@Override
		public void open(Configuration parameters) throws Exception {
			MapStateDescriptor<String, Integer> mapDesc = new MapStateDescriptor<>("Map", String.class,
					Integer.class);
			mapDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.minutes(100)).build());
			mapState = getRuntimeContext().getMapState(mapDesc);

			ListStateDescriptor<Integer> listDesc = new ListStateDescriptor<>("List", Integer.class);
			listDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.minutes(100)).build());
			listState = getRuntimeContext().getListState(listDesc);

			ValueStateDescriptor<Integer> valueStateDesc = new ValueStateDescriptor<Integer>("Val", Integer.class);
			valueStateDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.minutes(100)).build());
			valueState = getRuntimeContext().getState(valueStateDesc);
		}

		@Override
		public String map(Tuple2<Integer, Integer> value) throws Exception {
			mapState.put(value.f1.toString(), Optional.ofNullable(mapState.get(value.f1.toString())).orElse(0) + 1);
			listState.add(value.f1);
			valueState.update(value.f1);
			return "";
		}
	}

}
