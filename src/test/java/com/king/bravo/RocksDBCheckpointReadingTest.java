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
import java.util.HashSet;
import java.util.Optional;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.king.bravo.reader.KeyedStateReader;
import com.king.bravo.reader.OperatorStateReader;
import com.king.bravo.testing.BravoTestPipeline;

public class RocksDBCheckpointReadingTest extends BravoTestPipeline {

	private static final long serialVersionUID = 1L;

	@Test
	public void test() throws Exception {
		process("1");
		process("2");
		process("1");
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

		DataSet<Tuple2<Integer, Integer>> countState = reader.readKeyedStates(
				KeyedStateReader.forValueStateKVPairs("Count", new TypeHint<Tuple2<Integer, Integer>>() {}));

		assertEquals(Sets.newHashSet(Tuple2.of(1, 2), Tuple2.of(2, 1)), new HashSet<>(countState.collect()));
	}

	public DataStream<String> constructTestPipeline(DataStream<String> source) {
		return source
				.map(Integer::parseInt)
				.returns(Integer.class)
				.keyBy(i -> i)
				.map(new StatefulCounter())
				.uid("hello")
				.map(Tuple2::toString)
				.returns(String.class);
	}

	public static class StatefulCounter extends RichMapFunction<Integer, Tuple2<Integer, Integer>> {

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

}
