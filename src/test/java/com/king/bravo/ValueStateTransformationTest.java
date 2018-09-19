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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.king.bravo.reader.KeyedStateReader;
import com.king.bravo.reader.OperatorStateReader;
import com.king.bravo.testing.BravoTestPipeline;
import com.king.bravo.utils.StateMetadataUtils;
import com.king.bravo.writer.OperatorStateWriter;

public class ValueStateTransformationTest extends BravoTestPipeline {

	private static final long serialVersionUID = 1L;

	@Test
	public void test() throws Exception {
		process("1");
		process("2");
		process("1");
		triggerSavepoint();
		List<String> output = runTestPipeline(this::constructTestPipeline);
		assertEquals(Sets.newHashSet("(1,1)", "(2,1)", "(1,2)"), new HashSet<>(output));
		Path newSavepointPath = transformLastSavepoint();

		process("1");
		process("2");
		List<String> restoredOutput = restoreTestPipelineFromSnapshot(newSavepointPath.getPath(),
				this::restoreTestPipeline);
		assertEquals(Sets.newHashSet("(1,1)", "(2,1)", "(1,2)", "(1,5,103)", "(2,3,1002)"),
				new HashSet<>(restoredOutput));
	}

	private Path transformLastSavepoint() throws IOException, Exception {
		ExecutionEnvironment environment = ExecutionEnvironment.createLocalEnvironment();
		Savepoint savepoint = getLastSavepoint();
		OperatorStateReader reader = new OperatorStateReader(environment, savepoint, "hello");

		DataSet<Tuple2<Integer, Integer>> countState = reader.readKeyedStates(
				KeyedStateReader.forValueStateKVPairs("Count", new TypeHint<Tuple2<Integer, Integer>>() {}));

		DataSet<Tuple2<Integer, Integer>> newCountsToAdd = environment
				.fromElements(Tuple2.of(0, 100), Tuple2.of(3, 1000), Tuple2.of(1, 100), Tuple2.of(2, 1000));

		DataSet<Tuple2<Integer, Integer>> newStates = countState.join(newCountsToAdd).where(0).equalTo(0)
				.map(new SumValues());

		Path newCheckpointBasePath = new Path(getCheckpointDir(), "new");
		OperatorStateWriter operatorStateWriter = new OperatorStateWriter(savepoint, "hello", newCheckpointBasePath);

		operatorStateWriter.addValueState("Count",
				countState.map(t -> Tuple2.of(t.f0, t.f1 * 2)).returns(new TypeHint<Tuple2<Integer, Integer>>() {}));

		operatorStateWriter.createNewValueState("Count2", newStates, IntSerializer.INSTANCE);
		operatorStateWriter.addKeyedStateRows(reader.getAllUnreadKeyedStateRows());

		OperatorState newOpState = operatorStateWriter.writeAll();
		Savepoint newSavepoint = StateMetadataUtils.createNewSavepoint(savepoint, newOpState);
		StateMetadataUtils.writeSavepointMetadata(newCheckpointBasePath, newSavepoint);
		return newCheckpointBasePath;
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

	public DataStream<String> restoreTestPipeline(DataStream<String> source) {
		return source
				.map(Integer::parseInt)
				.returns(Integer.class)
				.keyBy(i -> i)
				.map(new StatefulCounter2())
				.uid("hello").map(Tuple3::toString)
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

	public static class StatefulCounter2 extends RichMapFunction<Integer, Tuple3<Integer, Integer, Integer>> {

		private static final long serialVersionUID = 7317800376639115920L;
		private ValueState<Integer> count;
		private ValueState<Integer> count2;

		@Override
		public void open(Configuration parameters) throws Exception {
			count = getRuntimeContext().getState(new ValueStateDescriptor<>("Count", Integer.class));
			count2 = getRuntimeContext().getState(new ValueStateDescriptor<>("Count2", Integer.class));
		}

		@Override
		public Tuple3<Integer, Integer, Integer> map(Integer value) throws Exception {
			count.update(Optional.ofNullable(count.value()).orElse(0) + 1);
			count2.update(Optional.ofNullable(count2.value()).orElse(0) + 1);
			return Tuple3.of(value, count.value(), count2.value());
		}
	}

	public static class SumValues implements
			MapFunction<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, Tuple2<Integer, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Integer, Integer> map(Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> t)
				throws Exception {
			return Tuple2.of(t.f0.f0, t.f0.f1 + t.f1.f1);
		}
	}
}
