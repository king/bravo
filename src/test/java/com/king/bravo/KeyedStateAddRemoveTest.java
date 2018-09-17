package com.king.bravo;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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
import com.king.bravo.testing.BravoTestPipeline;
import com.king.bravo.utils.StateMetadataUtils;
import com.king.bravo.writer.OperatorStateWriter;

public class KeyedStateAddRemoveTest extends BravoTestPipeline {

	private static final long serialVersionUID = 1L;

	@Test
	public void test() throws Exception {
		process("1");
		process("2");
		// Ignored by the statful filter...
		process("2");
		process("1");
		process("2");
		triggerSavepoint();
		List<String> output = runTestPipeline(this::constructTestPipeline);
		assertEquals(Sets.newHashSet("(1,0)", "(2,0)"), new HashSet<>(output));
		Path newSavepointPath = transformLastSavepoint();

		// Filter state is dropped, should process this now
		process("1");
		process("2");
		List<String> restoredOutput = restoreTestPipelineFromSavepoint(newSavepointPath.getPath(),
				this::restoreTestPipeline);
		assertEquals(Sets.newHashSet("(1,0)", "(2,0)", "(1,101)", "(2,101)"),
				new HashSet<>(restoredOutput));
	}

	private Path transformLastSavepoint() throws IOException, Exception {
		ExecutionEnvironment environment = ExecutionEnvironment.createLocalEnvironment();
		Savepoint savepoint = getLastSavepoint();

		DataSet<Tuple2<Integer, Integer>> bootstrapState = environment.fromElements(Tuple2.of(1, 100),
				Tuple2.of(2, 100));

		Path newCheckpointBasePath = new Path(getCheckpointDir(), "new");

		OperatorStateWriter counterStateWriter = new OperatorStateWriter(savepoint, "counter", newCheckpointBasePath);

		counterStateWriter.setKeySerializer(IntSerializer.INSTANCE);
		counterStateWriter.createNewValueState("count", bootstrapState, IntSerializer.INSTANCE);

		OperatorStateWriter filterStateWriter = new OperatorStateWriter(savepoint, "filter", newCheckpointBasePath);
		filterStateWriter.deleteKeyedState("seen");

		Savepoint newSavepoint = StateMetadataUtils.createNewSavepoint(savepoint,
				filterStateWriter.writeAll(),
				counterStateWriter.writeAll());
		StateMetadataUtils.writeSavepointMetadata(newCheckpointBasePath, newSavepoint);
		return newCheckpointBasePath;
	}

	public DataStream<String> constructTestPipeline(DataStream<String> source) {
		return source
				.map(Integer::parseInt)
				.returns(Integer.class)
				.keyBy(i -> i)
				.filter(new StatefulFilter())
				.uid("filter")
				.keyBy(i -> i)
				.map(new StatelessMap())
				.uid("counter")
				.map(new MapFunction<Tuple2<Integer, Integer>, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public String map(Tuple2<Integer, Integer> t) throws Exception {
						return t.toString();
					}
				});
	}

	public DataStream<String> restoreTestPipeline(DataStream<String> source) {
		return source
				.map(Integer::parseInt)
				.returns(Integer.class)
				.keyBy(i -> i)
				.filter(new StatefulFilter())
				.uid("filter")
				.keyBy(i -> i)
				.map(new StatefulCounter())
				.uid("counter").map(new MapFunction<Tuple2<Integer, Integer>, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public String map(Tuple2<Integer, Integer> t) throws Exception {
						return t.toString();
					}
				});
	}

	public static class StatefulFilter extends RichFilterFunction<Integer> {
		private static final long serialVersionUID = 1L;
		private ValueState<Boolean> seen;

		@Override
		public boolean filter(Integer value) throws Exception {
			if (seen.value() == null) {
				seen.update(true);
				return true;
			}
			return false;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			seen = getRuntimeContext().getState(new ValueStateDescriptor<>("seen", Boolean.class));
		}
	}

	public static class StatefulCounter extends RichMapFunction<Integer, Tuple2<Integer, Integer>> {

		private static final long serialVersionUID = 7317800376639115920L;
		private ValueState<Integer> count;

		@Override
		public void open(Configuration parameters) throws Exception {
			count = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Integer.class));
		}

		@Override
		public Tuple2<Integer, Integer> map(Integer value) throws Exception {
			count.update(Optional.ofNullable(count.value()).orElse(0) + 1);
			return Tuple2.of(value, count.value());
		}
	}

	public static class StatelessMap extends RichMapFunction<Integer, Tuple2<Integer, Integer>> {

		private static final long serialVersionUID = 7317800376639115920L;

		@Override
		public Tuple2<Integer, Integer> map(Integer value) throws Exception {
			return Tuple2.of(value, 0);
		}
	}
}
