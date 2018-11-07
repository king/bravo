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
import java.util.List;
import java.util.Optional;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.king.bravo.reader.KeyedStateReader;
import com.king.bravo.reader.OperatorStateReader;
import com.king.bravo.testing.BravoTestPipeline;
import com.king.bravo.utils.StateMetadataUtils;
import com.king.bravo.writer.OperatorStateWriter;

public class ValueStateTypeChangeTest extends BravoTestPipeline {

	private static final long serialVersionUID = 1L;

	@Test
	public void test() throws Exception {
		process("1");
		process("1");
		process("3");
		process("1");
		process("3");
		triggerSavepoint();
		List<String> output = runTestPipeline(this::pipelineWithStringState);
		assertEquals(
				Sets.newHashSet("(1,1)", "(1,2)", "(1,3)", "(3,3)", "(3,6)"), new HashSet<>(output));
		Path newSavepointPath = transformLastSavepoint();

		process("1");
		process("2");
		process("3");

		List<String> restoredOutput = restoreTestPipelineFromSnapshot(newSavepointPath.getPath(),
				this::pipelineWithIntState);
		assertEquals(Sets.newHashSet("(1,1)", "(1,2)", "(1,3)", "(3,3)", "(3,6)", "(1,4)", "(3,9)", "(2,2)"),
				new HashSet<>(restoredOutput));
	}

	private Path transformLastSavepoint() throws IOException, Exception {
		ExecutionEnvironment environment = ExecutionEnvironment.createLocalEnvironment();
		Savepoint savepoint = getLastSavepoint();
		OperatorStateReader reader = new OperatorStateReader(environment, savepoint, "hello");

		DataSet<Tuple2<Integer, String>> sumStringState = reader.readKeyedStates(
				KeyedStateReader.forValueStateKVPairs("sum", new TypeHint<Tuple2<Integer, String>>() {}));

		DataSet<Tuple2<Integer, Integer>> sumIntState = sumStringState.map(t -> Tuple2.of(t.f0, Integer.parseInt(t.f1)))
				.returns(new TypeHint<Tuple2<Integer, Integer>>() {});

		Path newCheckpointBasePath = new Path(getCheckpointDir(), "new");

		OperatorStateWriter sumStateWriter = new OperatorStateWriter(savepoint, "hello", newCheckpointBasePath);
		sumStateWriter.createNewValueState("sum", sumIntState, IntSerializer.INSTANCE);

		Savepoint newSavepoint = StateMetadataUtils.createNewSavepoint(savepoint, sumStateWriter.writeAll());
		StateMetadataUtils.writeSavepointMetadata(newCheckpointBasePath, newSavepoint);
		return newCheckpointBasePath;
	}

	public DataStream<String> pipelineWithStringState(DataStream<String> source) {
		return source
				.map(Integer::parseInt)
				.returns(Integer.class)
				.keyBy(i -> i)
				.map(new MapWithStringState())
				.uid("hello")
				.map(Tuple2::toString);
	}

	public DataStream<String> pipelineWithIntState(DataStream<String> source) {
		return source
				.map(Integer::parseInt)
				.returns(Integer.class)
				.keyBy(i -> i)
				.map(new MapWithIntState())
				.uid("hello")
				.map(Tuple2::toString);
	}

	public static class MapWithStringState extends RichMapFunction<Integer, Tuple2<Integer, Integer>> {

		private static final long serialVersionUID = 7317800376639115920L;
		private ValueState<String> sum;

		@Override
		public void open(Configuration parameters) throws Exception {
			sum = getRuntimeContext().getState(new ValueStateDescriptor<>("sum", String.class));
		}

		@Override
		public Tuple2<Integer, Integer> map(Integer value) throws Exception {
			int current = Integer.parseInt(Optional.ofNullable(sum.value()).orElse("0"));
			int newVal = current + value;
			sum.update(Integer.toString(newVal));
			return Tuple2.of(value, newVal);
		}
	}

	public static class MapWithIntState extends RichMapFunction<Integer, Tuple2<Integer, Integer>> {

		private static final long serialVersionUID = 7317800376639115920L;
		private ValueState<Integer> sum;

		@Override
		public void open(Configuration parameters) throws Exception {
			sum = getRuntimeContext().getState(new ValueStateDescriptor<>("sum", Integer.class));
		}

		@Override
		public Tuple2<Integer, Integer> map(Integer value) throws Exception {
			int current = Optional.ofNullable(sum.value()).orElse(0);
			int newVal = current + value;
			sum.update(newVal);
			return Tuple2.of(value, newVal);
		}
	}

}
