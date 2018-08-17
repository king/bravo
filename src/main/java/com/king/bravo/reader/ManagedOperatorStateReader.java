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
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Maps;

import com.king.bravo.utils.StateMetadataUtils;

public class ManagedOperatorStateReader {

	private OperatorState opState;

	public ManagedOperatorStateReader(OperatorState opState) {
		this.opState = opState;
	}

	public ManagedOperatorStateReader(Savepoint sp, String uid) {
		this(StateMetadataUtils.getOperatorState(sp, uid));
	}

	/**
	 * Restores the OperatorStateBackend corresponding to the given subtask. The
	 * backend is completely restored in-memory.
	 */
	public OperatorStateBackend createOperatorStateBackendFromSnapshot(int subtask) throws Exception {
		return restoreBackend(opState.getState(subtask).getManagedOperatorState());
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

	public static OperatorStateBackend restoreBackend(StateObjectCollection<OperatorStateHandle> managedOpState)
			throws Exception {

		DefaultOperatorStateBackend stateBackend = new DefaultOperatorStateBackend(
				ManagedOperatorStateReader.class.getClassLoader(), new ExecutionConfig(), false);

		stateBackend.restore(managedOpState);
		return stateBackend;
	}

	/**
	 * Restores the OperatorStateBackends corresponding to the different subtasks.
	 * The backends are completely restored in-memory.
	 */
	public Map<Integer, OperatorStateBackend> createOperatorStateBackendsFromSnapshot() throws Exception {
		return Maps.transformValues(opState.getSubtaskStates(),
				sst -> {
					try {
						return restoreBackend(sst.getManagedOperatorState());
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				});
	}
}
