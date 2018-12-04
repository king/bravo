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
package com.king.bravo.writer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.filesystem.FileBasedStateOutputStream;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;

import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.king.bravo.reader.OperatorStateReader;
import com.king.bravo.types.KeyedStateRow;
import com.king.bravo.utils.StateMetadataUtils;
import com.king.bravo.writer.functions.KeyGroupAndStateNameKey;
import com.king.bravo.writer.functions.OperatorIndexForKeyGroupKey;
import com.king.bravo.writer.functions.RocksDBSavepointWriter;
import com.king.bravo.writer.functions.RowFilter;
import com.king.bravo.writer.functions.ValueStateToKeyedStateRow;

/**
 * Utility for creating new OperatorStates based on old checkpointed data and
 * new state from datasets. This can be used to modify add and remove keyed
 * states as well as to modify non-keyed states.
 */
public class OperatorStateWriter {

	private OperatorState baseOpState;
	private DataSet<KeyedStateRow> allRows = null;
	private Path newCheckpointBasePath;
	private long checkpointId;
	private BiConsumer<Integer, OperatorStateBackend> transformer;
	private Map<String, StateMetaInfoSnapshot> metaSnapshots;
	private KeyedBackendSerializationProxy<?> proxy;

	private boolean keepBaseKeyedStates = true;

	private TypeSerializer<?> keySerializer = null;

	public OperatorStateWriter(Savepoint sp, String uid, Path newCheckpointBasePath) {
		this(sp.getCheckpointId(), StateMetadataUtils.getOperatorState(sp, uid), newCheckpointBasePath);
	}

	public OperatorStateWriter(long checkpointId, OperatorState baseOpState, Path newCheckpointBasePath) {
		this.baseOpState = baseOpState;
		this.newCheckpointBasePath = newCheckpointBasePath;
		this.checkpointId = checkpointId;

		proxy = StateMetadataUtils.getKeyedBackendSerializationProxy(baseOpState).orElse(null);
		metaSnapshots = new HashMap<>();
		if (proxy != null) {
			proxy.getStateMetaInfoSnapshots()
					.forEach(ms -> metaSnapshots.put(ms.getName(),
							new StateMetaInfoSnapshot(ms.getName(), ms.getBackendStateType(), ms.getOptionsImmutable(),
									ms.getSerializerConfigSnapshotsImmutable(),
									Maps.transformValues(ms.getSerializerConfigSnapshotsImmutable(),
											TypeSerializerSnapshot::restoreSerializer))));
		}
	}

	/**
	 * Defines the keyserializer for this operator. This method can be used when
	 * adding state to a previously stateless operator where the keyserializer
	 * is not available from the state.
	 * 
	 * @param keySerializer
	 */
	public void setKeySerializer(TypeSerializer<?> keySerializer) {
		this.keySerializer = keySerializer;
	}

	/**
	 * Add a Dataset of {@link KeyedStateRow}s to the state of the operator,
	 * this is mostly used to migrate existing states of the operator to the new
	 * operator state without modifications.
	 * <p>
	 * This can be used to add all different kinds of keyed states: value, list,
	 * map
	 * 
	 * @param rows
	 *            State rows to be added
	 */
	public void addKeyedStateRows(DataSet<KeyedStateRow> rows) {
		allRows = allRows == null ? rows : allRows.union(rows);
		keepBaseKeyedStates = false;
	}

	/**
	 * Removes the state metadata and rows for the given statename.
	 * 
	 * @param stateName
	 *            Name of the state to be deleted
	 */
	public void deleteKeyedState(String stateName) {
		metaSnapshots.remove(stateName);
		keepBaseKeyedStates = false;
	}

	private void updateProxy() {
		if (proxy == null && keySerializer == null) {
			throw new IllegalStateException(
					"KeySerializer must be defined when adding state to a previously stateless operator. Use writer.setKeySerializer(...)");
		}

		proxy = new KeyedBackendSerializationProxy<>(
				getKeySerializer(),
				new ArrayList<>(metaSnapshots.values()),
				proxy != null ? proxy.isUsingKeyGroupCompression() : true);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private <T> TypeSerializer<T> getKeySerializer() {
		return proxy != null ? (TypeSerializer) proxy.getKeySerializerConfigSnapshot().restoreSerializer()
				: (TypeSerializer) keySerializer;
	}

	/**
	 * Adds a dataset of K-V pairs to the keyed state of the operator. This
	 * operation assumes that a state with the same name is already defined and
	 * the metadata is reused.
	 * <p>
	 * To define new states see
	 * {@link #createNewValueState(String, DataSet, TypeSerializer)}
	 * <p>
	 * Keep in mind that any state rows for the same name already added (through
	 * {@link #addKeyedStateRows(DataSet)}) will not be overwritten.
	 * 
	 * @param stateName
	 * @param newState
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public <K, V> void addValueState(String stateName, DataSet<Tuple2<K, V>> newState) {
		TypeSerializer<V> valueSerializer = (TypeSerializer<V>) (TypeSerializer) StateMetadataUtils
				.getSerializer(proxy, stateName)
				.orElseThrow(
						() -> new IllegalArgumentException("Cannot find state " + stateName));

		if (StateMetadataUtils.isTtlState(valueSerializer)) {
			throw new RuntimeException("Writing of TTL states is not supported at the moment.");
		}
		addKeyedStateRows(newState
				.map(new ValueStateToKeyedStateRow<K, V>(stateName,
						getKeySerializer(),
						valueSerializer,
						baseOpState.getMaxParallelism())));
	}

	/**
	 * Defines/redefines a value state with the given name and type. This can be
	 * used to create new states of an operator or change the type of an already
	 * existing state.
	 * <p>
	 * When redefining a pre-existing state make sure you haven't added that as
	 * keyed state rows before.
	 * 
	 * @param stateName
	 * @param newState
	 * @param valueSerializer
	 */
	public <K, V> void createNewValueState(String stateName, DataSet<Tuple2<K, V>> newState,
			TypeSerializer<V> valueSerializer) {

		metaSnapshots.put(stateName, new RegisteredKeyValueStateBackendMetaInfo<>(StateDescriptor.Type.VALUE, stateName,
				VoidNamespaceSerializer.INSTANCE, valueSerializer).snapshot());

		updateProxy();
		addKeyedStateRows(newState
				.map(new ValueStateToKeyedStateRow<K, V>(stateName,
						getKeySerializer(),
						valueSerializer,
						baseOpState.getMaxParallelism())));
	}

	/**
	 * Triggers the batch processing operations to write the operator state data
	 * to persistent storage and create the metadata object
	 * 
	 * @return {@link OperatorState} metadata pointing to the newly written
	 *         state
	 */
	public OperatorState writeAll() throws Exception {
		int maxParallelism = baseOpState.getMaxParallelism();
		int parallelism = baseOpState.getParallelism();
		Path outDir = makeOutputDir();

		Map<Integer, KeyedStateHandle> handleMap = new HashMap<>();
		if (allRows == null) {
			if (!keepBaseKeyedStates && !metaSnapshots.isEmpty()) {
				throw new IllegalStateException(
						"States must be added when any modification of existing keyed states were made");
			} else {
				// Either keep all state or delete all (no need to do anything
				// here)
			}
		} else if (!metaSnapshots.isEmpty()) {
			updateProxy();
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			DataOutputView bow = new DataOutputViewStreamWrapper(os);
			proxy.write(bow);

			DataSet<Tuple2<Integer, KeyedStateHandle>> handles = allRows
					.filter(new RowFilter(metaSnapshots.keySet()))
					.groupBy(new OperatorIndexForKeyGroupKey(maxParallelism, parallelism))
					.sortGroup(new KeyGroupAndStateNameKey(maxParallelism), Order.ASCENDING)
					.reduceGroup(new RocksDBSavepointWriter(maxParallelism, parallelism,
							HashBiMap.create(StateMetadataUtils.getStateIdMapping(proxy)).inverse(),
							proxy.isUsingKeyGroupCompression(), outDir, os.toByteArray()));

			handles.collect().stream().forEach(t -> handleMap.put(t.f0, t.f1));
		} else {
			throw new IllegalStateException(
					"There are state rows but no state metadata... maybe you meant to use createNewValueState(...)");
		}

		// We construct a new operatorstate with the collected handles
		OperatorState newOperatorState = new OperatorState(baseOpState.getOperatorID(), parallelism, maxParallelism);

		// Fill with the subtaskstates based on the old one (we need to preserve
		// the
		// other states)
		baseOpState.getSubtaskStates().forEach((subtaskId, subtaskState) -> {
			KeyedStateHandle newKeyedHandle = handleMap.get(subtaskId);
			StateObjectCollection<OperatorStateHandle> opHandle = transformSubtaskOpState(outDir, subtaskId,
					subtaskState.getManagedOperatorState());

			newOperatorState.putState(subtaskId,
					new OperatorSubtaskState(
							opHandle,
							subtaskState.getRawOperatorState(),
							keepBaseKeyedStates ? subtaskState.getManagedKeyedState()
									: new StateObjectCollection<>(
											newKeyedHandle != null
													? Lists.newArrayList(newKeyedHandle)
													: Collections.emptyList()),
							subtaskState.getRawKeyedState()));
		});

		return newOperatorState;
	}

	private StateObjectCollection<OperatorStateHandle> transformSubtaskOpState(Path outDir, Integer subtaskId,
			StateObjectCollection<OperatorStateHandle> baseState) {

		if (transformer == null) {
			return baseState;
		}

		StateObjectCollection<OperatorStateHandle> opHandle = baseState;
		try (OperatorStateBackend opBackend = OperatorStateReader
				.restoreOperatorStateBackend(opHandle)) {

			transformer.accept(subtaskId, opBackend);

			OperatorStateHandle newSnapshot = opBackend
					.snapshot(checkpointId, System.currentTimeMillis(), new CheckpointStreamFactory() {
						@Override
						public CheckpointStateOutputStream createCheckpointStateOutputStream(
								CheckpointedStateScope scope)
								throws IOException {
							return new FileBasedStateOutputStream(outDir.getFileSystem(),
									new Path(outDir, String.valueOf(UUID.randomUUID())));
						}
					}, null).get().getJobManagerOwnedSnapshot();
			return new StateObjectCollection<>(Lists.newArrayList(newSnapshot));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Transform the non-keyed state of the operator by applying a function to
	 * the non-keyed state of each operator instance. Any update made to the
	 * {@link OperatorStateBackend} will be stored back as the new state of the
	 * operator.
	 * <p>
	 * The transformation will be executed sequentially, in-memory on the
	 * client.
	 * 
	 * @param transformer
	 *            Consumer to be applied on the {@link OperatorStateBackend}
	 * @throws Exception
	 */
	public <K, V> void transformNonKeyedState(BiConsumer<Integer, OperatorStateBackend> transformer) throws Exception {
		this.transformer = transformer;
	}

	private Path makeOutputDir() {
		final Path outDir = new Path(new Path(newCheckpointBasePath, "mchk-" + checkpointId),
				"op-" + baseOpState.getOperatorID());
		try {
			outDir.getFileSystem().mkdirs(outDir);
		} catch (IOException ignore) {}
		return outDir;
	}

}
