package com.king.bravo;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.king.bravo.reader.KeyedStateReader;
import com.king.bravo.reader.OperatorStateReader;
import com.king.bravo.testing.BravoTestPipeline;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.api.java.ExecutionEnvironment.createLocalEnvironment;
import static org.junit.Assert.assertEquals;

public class WindowStateReadingTest extends BravoTestPipeline {

    private static final long serialVersionUID = 1L;
    private static final String REDUCER_UID = "test-reducer";
    private static final MapTypeInfo<String, String> MAP_TYPE_INFO = new MapTypeInfo<>(String.class, String.class);

    @Test
    public void readReducerState() throws Exception {
        Arrays.asList("1,1", "2,3", "1,2", "1,1").stream().forEach(this::process);
        sleep(1000);
        cancelJob();
        runTestPipeline(this::constructTestPipeline);
        OperatorStateReader reader = new OperatorStateReader(createLocalEnvironment(), getLastCheckpoint(),
                REDUCER_UID);
        assertReadReducerStateValues(reader);
        assertReadReducerStateKVPairs(reader);
    }

    private void assertReadReducerStateValues(OperatorStateReader reader) throws Exception {
        List<Map<String, String>> mapValues = reader.readKeyedStates(KeyedStateReader
                .forReducerStateValues(MAP_TYPE_INFO)).collect();

        assertEquals(
                ImmutableSet.of(
                        ImmutableMap.of("2", "3"),
                        ImmutableMap.of("1", "1")
                ), ImmutableSet.copyOf(mapValues));
    }

    private void assertReadReducerStateKVPairs(OperatorStateReader reader) throws Exception {
        List<Tuple2<String, Map<String, String>>> mapKeysAndValues = reader.readKeyedStates(KeyedStateReader
                .forReducerStateKVPairs(BasicTypeInfo.STRING_TYPE_INFO, MAP_TYPE_INFO)).collect();

        assertEquals(
                ImmutableMap.of(
                        "2", ImmutableMap.of("2", "3"),
                        "1", ImmutableMap.of("1", "1")
                ), mapKeysAndValues.stream().collect(Collectors.toMap(t -> t.f0, t -> t.f1)));
    }

    public DataStream<String> constructTestPipeline(DataStream<String> source) {
        return source
                .map(s -> new HashMap<>(ImmutableMap.of(s.split(",")[0], s.split(",")[1])))
                .returns(new TypeHint<HashMap<String, String>>() {})
                .keyBy(map -> map.keySet().iterator().next())
                .timeWindow(Time.milliseconds(1))
                .reduce((l, r) -> l.values().iterator().next().compareTo(r.values().iterator().next()) > 0 ? r : l)
                .uid(REDUCER_UID)
                // convert output type to be compatible with BravoTestPipeline#runTestPipeline
                .map(Map::toString);
    }

}
