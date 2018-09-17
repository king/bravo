package com.king.bravo;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
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

public class StateAddTransformTest extends BravoTestPipeline {

	private static final long serialVersionUID = 1L;

	@Test
	public void test() throws Exception {
		process("1");
		process("2");
		process("1");
		triggerSavepoint();
		List<String> output = runTestPipeline();
		assertEquals(Sets.newHashSet("(1,1)", "(2,1)", "(1,2)"), new HashSet<>(output));
		Path newSavepointPath = transformLastSavepoint();

		process("1");
		process("2");
		List<String> restoredOutput = restoreTestPipelineFromSavepoint(newSavepointPath.getPath());
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

	@Override
	public DataStream<String> constructTestPipeline(DataStream<String> source) {
		return source
				.map(Integer::parseInt)
				.returns(Integer.class)
				.keyBy(i -> i)
				.map(new Counter())
				.uid("hello")
				.map(new MapFunction<Tuple2<Integer, Integer>, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public String map(Tuple2<Integer, Integer> t) throws Exception {
						return t.toString();
					}
				});
	}

	@Override
	public DataStream<String> restoreTestPipeline(DataStream<String> source) {
		return source
				.map(Integer::parseInt)
				.returns(Integer.class)
				.keyBy(i -> i)
				.map(new Counter2())
				.uid("hello").map(new MapFunction<Tuple3<Integer, Integer, Integer>, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public String map(Tuple3<Integer, Integer, Integer> t) throws Exception {
						return t.toString();
					}
				});
	}
}
