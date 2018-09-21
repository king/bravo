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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.Test;

import com.king.bravo.reader.OperatorStateReader;
import com.king.bravo.testing.BravoTestPipeline;
import com.king.bravo.utils.StateMetadataUtils;
import com.king.bravo.writer.OperatorStateWriter;

public class BroadcastStateTransformationTest extends BravoTestPipeline {

	private static final long serialVersionUID = 1L;

	static MapStateDescriptor<Boolean, List<Integer>> bcstate = new MapStateDescriptor<>("filteredKeys",
			BasicTypeInfo.BOOLEAN_TYPE_INFO, new ListTypeInfo<>(Integer.class));

	@Test
	public void test() throws Exception {
		process("process 1");
		sleep(500);
		process("filter 1");
		sleep(1000);
		process("process 1");
		process("process 2");
		process("process 3");
		sleep(1000);
		cancelJob();
		List<String> output = runTestPipeline(this::constructTestPipeline);
		assertEquals(3, output.size());

		process("process 1");
		process("process 2");
		process("process 3");
		triggerSavepoint();

		System.err.println("Round two");
		List<String> output2 = restoreTestPipelineFromSnapshot(
				validateStateAndTransform(getLastCheckpoint()).getPath(),
				this::constructTestPipeline);

		Savepoint savepoint = getLastSavepoint();

		validateSecondSavepoint(savepoint);
		assertEquals(4, output2.size());
	}

	private Path validateStateAndTransform(Savepoint savepoint) throws IOException, Exception {
		ExecutionEnvironment environment = ExecutionEnvironment.createLocalEnvironment();

		// Validate the contents of the broadcast state
		OperatorStateReader reader = new OperatorStateReader(environment, savepoint, "stateful");
		OperatorStateBackend backend = reader.createOperatorStateBackendFromSnapshot(1);
		BroadcastState<Boolean, List<Integer>> state = backend.getBroadcastState(bcstate);
		assertEquals(Lists.newArrayList(1), state.get(true));

		Path newCheckpointBasePath = new Path(getCheckpointDir(), "new");
		OperatorStateWriter writer = new OperatorStateWriter(savepoint, "stateful", newCheckpointBasePath);
		writer.transformNonKeyedState((i, b) -> {
			try {
				b.getBroadcastState(bcstate).put(true, Lists.newArrayList(2, 3));
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		});

		StateMetadataUtils.writeSavepointMetadata(newCheckpointBasePath,
				StateMetadataUtils.createNewSavepoint(savepoint, writer.writeAll()));
		return newCheckpointBasePath;
	}

	private void validateSecondSavepoint(Savepoint savepoint) throws Exception {
		// Validate the contents of the broadcast state
		OperatorStateReader reader = new OperatorStateReader(ExecutionEnvironment.createLocalEnvironment(), savepoint,
				"stateful");
		OperatorStateBackend backend = reader.createOperatorStateBackendFromSnapshot(1);
		BroadcastState<Boolean, List<Integer>> state = backend.getBroadcastState(bcstate);
		assertEquals(Lists.newArrayList(2, 3), state.get(true));
	}

	public DataStream<String> constructTestPipeline(DataStream<String> source) {

		OutputTag<Integer> filtered = new OutputTag<>("filter", BasicTypeInfo.INT_TYPE_INFO);
		OutputTag<Integer> process = new OutputTag<>("process", BasicTypeInfo.INT_TYPE_INFO);

		SingleOutputStreamOperator<String> input = source.process(new ProcessFunction<String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void processElement(String s, Context ctx,
					Collector<String> out) throws Exception {

				if (s.startsWith("filter ")) {
					ctx.output(filtered, Integer.parseInt(s.substring(7)));
				} else if (s.startsWith("process ")) {
					ctx.output(process, Integer.parseInt(s.substring(8)));
				} else {
					throw new RuntimeException("oOoO");
				}

			}
		});

		BroadcastStream<Integer> broadcast = input.getSideOutput(filtered).broadcast(bcstate);

		return input.getSideOutput(process).keyBy(i -> i).connect(broadcast).process(new BroadcastProcessor(bcstate))
				.uid("stateful");
	}

	public static class BroadcastProcessor extends KeyedBroadcastProcessFunction<Integer, Integer, Integer, String> {

		private static final long serialVersionUID = 7317800376639115920L;
		private MapStateDescriptor<Boolean, List<Integer>> desc;

		public BroadcastProcessor(MapStateDescriptor<Boolean, List<Integer>> desc) {
			this.desc = desc;
		}

		@Override
		public void processElement(Integer i, ReadOnlyContext ctx, Collector<String> out) throws Exception {
			List<Integer> filtered = Optional.ofNullable(ctx.getBroadcastState(desc).get(true))
					.orElse(new ArrayList<>());
			if (!filtered.contains(i)) {
				out.collect(i.toString());
			}
		}

		@Override
		public void processBroadcastElement(Integer i, Context ctx, Collector<String> out) throws Exception {
			List<Integer> keys = ctx.getBroadcastState(desc).get(true);
			if (keys == null) {
				keys = new ArrayList<>();
			}
			if (!keys.contains(i)) {
				keys.add(i);
				ctx.getBroadcastState(desc).put(true, keys);
			}
		}
	}

}
