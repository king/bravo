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
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
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

public class MapStateReadingTest extends BravoTestPipeline {

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
				.readKeyedStates(KeyedStateReader.forMapStateEntries("Count", BasicTypeInfo.INT_TYPE_INFO,
						BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO)
						.withOutputTypesForDeserialization())
				.collect();

		List<Integer> mapValues = reader
				.readKeyedStates(KeyedStateReader.forMapStateValues("Count", BasicTypeInfo.INT_TYPE_INFO))
				.collect();

		assertEquals(Sets.newHashSet(Tuple3.of(1, "1", 2), Tuple3.of(1, "2", 1), Tuple3.of(2, "3", 1)),
				new HashSet<>(countState));

		Collections.sort(mapValues);
		assertEquals(Lists.newArrayList(1, 1, 2), mapValues);
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
		private MapState<String, Integer> count;

		@Override
		public void open(Configuration parameters) throws Exception {
			count = getRuntimeContext().getMapState(new MapStateDescriptor<>("Count", String.class, Integer.class));
		}

		@Override
		public String map(Tuple2<Integer, Integer> value) throws Exception {
			count.put(value.f1.toString(), Optional.ofNullable(count.get(value.f1.toString())).orElse(0) + 1);
			return "";
		}
	}

}
