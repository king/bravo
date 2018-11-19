package com.king.bravo;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.king.bravo.reader.KeyedStateReader;
import com.king.bravo.reader.OperatorStateReader;
import com.king.bravo.testing.BravoTestPipeline;
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
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

@Ignore
public class ReducingStateReadingTest extends BravoTestPipeline {

    private static final long serialVersionUID = 1L;
    public static final String REDUCER_UID = "test-reducer";
    // TODO where is this set in Flink code?
    public static final String REDUCER_STATE_NAME = "window-contents";

    @Test
    public void test() throws Exception {
        process("1,1");
        process("2,3");
        process("1,2");
        process("1,1");
        // TODO use a countdown latch somewhere?
        sleep(5000);
        cancelJob();
        runTestPipeline(this::constructTestPipeline);
        validateCheckpointedStateReading();
    }

    @SuppressWarnings("unchecked")
    private void validateCheckpointedStateReading() throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.createLocalEnvironment();
        Savepoint savepoint = getLastCheckpoint();
        OperatorStateReader reader = new OperatorStateReader(environment, savepoint, REDUCER_UID);

        List<Tuple3<Integer, String, Integer>> countState = reader
                .readKeyedStates(KeyedStateReader.forMapStateEntries(REDUCER_STATE_NAME, BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO))
                .collect();

        List<Integer> mapValues = reader
                .readKeyedStates(KeyedStateReader.forMapStateValues(REDUCER_STATE_NAME, BasicTypeInfo.INT_TYPE_INFO))
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
                    return new HashMap<>(ImmutableMap.of(split[0], split[1]));
                })
                .returns(new TypeHint<HashMap<String, String>>() {})
                .keyBy(map -> map.keySet().iterator().next())
                .timeWindow(Time.seconds(1))
                .reduce((v1, v2) -> {
                    System.err.println("v1 = " + v1);
                    return v1;
                })
                .uid(REDUCER_UID)
                .map(map -> Tuple2.of(map.keySet().iterator().next(), map.values().iterator().next()))
                .returns(new TypeHint<Tuple2<String, String>>() {})
                .keyBy(0)
                .map(new MapCounter())
                .uid("statefulmapper");
    }

    public static class MapCounter extends RichMapFunction<Tuple2<String, String>, String> {

        private static final long serialVersionUID = 7317800376639115920L;
        private MapState<String, String> count;

        @Override
        public void open(Configuration parameters) {
            MapStateDescriptor<String, String> descriptor = new MapStateDescriptor<>("Count", String.class,
                    String.class);
            count = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public String map(Tuple2<String, String> value) throws Exception {
            count.put(value.f1, Optional.ofNullable(count.get(value.f1)).orElse("1") + "1");
            return "";
        }
    }

}
