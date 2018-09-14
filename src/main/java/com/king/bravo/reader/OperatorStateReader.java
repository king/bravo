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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Maps;

import com.king.bravo.reader.inputformat.RocksDBKeyedStateInputFormat;
import com.king.bravo.types.KeyedStateRow;
import com.king.bravo.utils.StateMetadataUtils;

public class OperatorStateReader {

	private OperatorState opState;

	private DataSet<KeyedStateRow> allKeyedStateRows;

	private FilterFunction<String> keyedStateFilter;

	private ExecutionEnvironment env;

	private final HashSet<String> parsedStates = new HashSet<>();

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
	 * Read the states using the provided reader for further processing
	 * 
	 * @return The DataSet containing the deseralized state keys and values
	 *         depending on the reader
	 */
	public <K, V, O> DataSet<O> readKeyedStates(KeyedStateReader<K, V, O> reader) throws Exception {
		readKeyedStates();
		KeyedBackendSerializationProxy<?> proxy = StateMetadataUtils.getKeyedBackendSerializationProxy(opState);
		reader.configure(opState.getMaxParallelism(), proxy.getKeySerializer(),
				StateMetadataUtils.getSerializer(proxy, reader.getStateName())
						.orElseThrow(() -> new IllegalArgumentException("Cannot find state " + reader.getStateName())));
		DataSet<O> parsedState = allKeyedStateRows.flatMap(reader);
		parsedStates.add(reader.getStateName());
		return parsedState;
	}

	/**
	 * @return DataSet containing all keyed states of the operator
	 */
	public DataSet<KeyedStateRow> getAllKeyedStateRows() {
		readKeyedStates();
		return allKeyedStateRows;
	}

	/**
	 * Return all the keyed state rows that were not accessed using a reader. This
	 * is a convenience method so we can union the untouched part of the state with
	 * the changed parts before writing them back.
	 */
	public DataSet<KeyedStateRow> getAllUnreadKeyedStateRows() {
		readKeyedStates();
		HashSet<String> parsed = new HashSet<>(parsedStates);
		return allKeyedStateRows.filter(row -> !parsed.contains(row.f0));
	}

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
	 * Restores the OperatorStateBackends corresponding to the different subtasks.
	 * The backends are completely restored in-memory.
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

		DefaultOperatorStateBackend stateBackend = new DefaultOperatorStateBackend(
				OperatorStateReader.class.getClassLoader(), new ExecutionConfig(), false);

		stateBackend.restore(managedOpState);
		return stateBackend;
	}
}
