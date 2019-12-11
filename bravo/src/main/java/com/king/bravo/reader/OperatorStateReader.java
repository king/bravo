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
package com.king.bravo.reader;

import com.king.bravo.reader.inputformat.RocksDBKeyedStateInputFormat;
import com.king.bravo.types.KeyedStateRow;
import com.king.bravo.utils.StateMetadataUtils;
import com.king.bravo.writer.OperatorStateWriter;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializerBase;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.DefaultOperatorStateBackendBuilder;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utility for reading the states stored in a Flink operator into DataSets.
 * There are methods for reading both keyed and non-keyed states.
 *
 */
public class OperatorStateReader {

	private final OperatorState opState;
	private final FilterFunction<String> keyedStateFilter;
	private final ExecutionEnvironment env;

	private final HashSet<String> readStates = new HashSet<>();

	private DataSet<KeyedStateRow> allKeyedStateRows;

	private OperatorStateReader(ExecutionEnvironment env, OperatorState opState,
			FilterFunction<String> keyedStateFilter) {
		this.env = env;
		this.opState = opState;
		this.keyedStateFilter = keyedStateFilter;
	}

	public OperatorStateReader(ExecutionEnvironment env, Savepoint sp, String uid) {
		this(env, sp, uid, s -> true);
	}

	public OperatorStateReader(ExecutionEnvironment env, Savepoint sp, String uid, Collection<String> stateNames) {
		this(env, sp, uid, new FilterFunction<String>() {
			private static final long serialVersionUID = 1L;
			HashSet<String> filtered = new HashSet<>(stateNames);

			@Override
			public boolean filter(String s) throws Exception {
				return filtered.contains(s);
			}
		});
	}

	public OperatorStateReader(ExecutionEnvironment env, Savepoint sp, String uid,
			FilterFunction<String> keyedStateFilter) {
		this(env, StateMetadataUtils.getOperatorState(sp, uid), keyedStateFilter);
	}

	/**
	 * Read keyed states using the provided reader for further processing
	 * 
	 * @return The DataSet containing the deseralized state elements
	 */
	public <K, V, O> DataSet<O> readKeyedStates(KeyedStateReader<K, V, O> reader) throws Exception {
		readKeyedStates();
		KeyedBackendSerializationProxy<?> proxy = StateMetadataUtils.getKeyedBackendSerializationProxy(opState)
				.orElseThrow(() -> new IllegalStateException("Cannot read state of a stateless operator."));
		reader.configure(opState.getMaxParallelism(), getKeySerializer(proxy),
				StateMetadataUtils.getSerializer(proxy, reader.getStateName())
						.orElseThrow(() -> new IllegalArgumentException("Cannot find state " + reader.getStateName())));
		DataSet<O> parsedState = allKeyedStateRows.flatMap(reader);
		readStates.add(reader.getStateName());
		return parsedState;
	}

	private TypeSerializer<?> getKeySerializer(KeyedBackendSerializationProxy<?> proxy) {
		TypeSerializer<?> keySerializer = proxy.getKeySerializerSnapshot().restoreSerializer();
		if (keySerializer instanceof TupleSerializerBase) {
			TupleSerializerBase ts = (TupleSerializerBase) keySerializer;
			if (ts.getTupleClass().equals(Tuple1.class)) {
				return ts.getFieldSerializers()[0];
			}
		}
		return keySerializer;
	}

	/**
	 * @return DataSet containing all keyed states of the operator in a
	 *         serialized form
	 */
	public DataSet<KeyedStateRow> getAllKeyedStateRows() {
		readKeyedStates();
		return allKeyedStateRows;
	}

	/**
	 * Return all the keyed state rows that were not accessed using a reader.
	 * This is a convenience method so we can union the untouched part of the
	 * state with the changed parts before writing them back using the
	 * {@link OperatorStateWriter}.
	 */
	public DataSet<KeyedStateRow> getAllUnreadKeyedStateRows() {
		readKeyedStates();
		HashSet<String> parsed = new HashSet<>(readStates);
		return allKeyedStateRows.filter(row -> !parsed.contains(row.f0));
	}

	/**
	 * Get the serialized keyed state rows for the specified state names
	 * 
	 * @param stateNames
	 * @return The dataset of the rows
	 */
	public DataSet<KeyedStateRow> getKeyedStateRows(Set<String> stateNames) {
		readKeyedStates();
		HashSet<String> filtered = new HashSet<>(stateNames);
		return allKeyedStateRows.filter(row -> filtered.contains(row.f0));
	}

	private void readKeyedStates() {
		if (allKeyedStateRows == null) {
			allKeyedStateRows = env.createInput(new RocksDBKeyedStateInputFormat(opState, keyedStateFilter));
		}
	}

	/**
	 * Restores the OperatorStateBackend corresponding to the given subtask. The
	 * backend is completely restored in-memory.
	 */
	public OperatorStateBackend createOperatorStateBackendFromSnapshot(int subtask) throws Exception {
		return restoreOperatorStateBackend(opState.getState(subtask).getManagedOperatorState());
	}

	/**
	 * Read the serializableListState stored in the checkpoint for the given
	 * operator subtask
	 */
	public List<Serializable> getSerializableListState(int subtask) throws Exception {
		OperatorStateBackend backend = createOperatorStateBackendFromSnapshot(subtask);
		@SuppressWarnings("deprecation")
		ListState<Serializable> listState = backend
				.getSerializableListState(DefaultOperatorStateBackend.DEFAULT_OPERATOR_STATE_NAME);

		List<Serializable> list = new ArrayList<>();

		for (Serializable serializable : listState.get()) {
			list.add(serializable);
		}

		return list;
	}

	/**
	 * Restores the OperatorStateBackends corresponding to the different
	 * subtasks. The backends are completely restored in-memory.
	 */
	public Map<Integer, OperatorStateBackend> createOperatorStateBackendsFromSnapshot() throws Exception {
		return Maps.transformValues(opState.getSubtaskStates(),
				sst -> {
					try {
						return restoreOperatorStateBackend(sst.getManagedOperatorState());
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				});
	}

	public static OperatorStateBackend restoreOperatorStateBackend(
			StateObjectCollection<OperatorStateHandle> managedOpState)
			throws Exception {

		return new DefaultOperatorStateBackendBuilder(
				OperatorStateReader.class.getClassLoader(), new ExecutionConfig(), false,
				managedOpState, new CloseableRegistry()).build();
	}
}
