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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.filesystem.FileBasedStateOutputStream;

import com.google.common.collect.Lists;
import com.king.bravo.api.KeyedStateRow;
import com.king.bravo.reader.ManagedOperatorStateReader;
import com.king.bravo.utils.SerializableSupplier;
import com.king.bravo.utils.StateMetadataUtils;
import com.king.bravo.writer.functions.KeyGroupAndStateIdKey;
import com.king.bravo.writer.functions.KeyedStateWriterFunction;
import com.king.bravo.writer.functions.MapToKeyedStateRow;
import com.king.bravo.writer.functions.OperatorIndexForKeyGroupKey;

public class StateTransformer {

	private final long checkpointId;
	private final Path newCheckpointBasePath;
	private Savepoint savepoint;

	public StateTransformer(Savepoint oldSavepoint, Path newCheckpointBasePath) {
		this.savepoint = oldSavepoint;
		this.checkpointId = oldSavepoint.getCheckpointId();
		this.newCheckpointBasePath = newCheckpointBasePath;
	}

	public Path getNewCheckpointPath() {
		return newCheckpointBasePath;
	}

	public Path writeSavepointMetadata() throws IOException {
		Path p = new Path(newCheckpointBasePath, "_metadata");
		Checkpoints.storeCheckpointMetadata(savepoint,
				FileSystem.getLocalFileSystem().create(p, WriteMode.NO_OVERWRITE));
		return p;
	}

	public Savepoint getSavepoint() {
		return savepoint;
	}

	public <K, V> void transformManagedOperatorState(String uid, BiConsumer<Integer, OperatorStateBackend> transformer)
			throws Exception {
		transformManagedOperatorState(StateMetadataUtils.getOperatorState(savepoint, uid), transformer);
	}

	public <K, V> void transformManagedOperatorState(OperatorState oldOpState,
			BiConsumer<Integer, OperatorStateBackend> transformer) throws Exception {

		OperatorState transformedOperatorState = new OperatorState(
				oldOpState.getOperatorID(),
				oldOpState.getParallelism(),
				oldOpState.getMaxParallelism());

		Path outDir = makeOutputDir(oldOpState.getOperatorID());

		for (Entry<Integer, OperatorSubtaskState> e : oldOpState.getSubtaskStates().entrySet()) {
			try (OperatorStateBackend opBackend = ManagedOperatorStateReader
					.restoreBackend(e.getValue().getManagedOperatorState())) {

				transformer.accept(e.getKey(), opBackend);

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

				transformedOperatorState.putState(e.getKey(),
						new OperatorSubtaskState(
								new StateObjectCollection<>(Lists.newArrayList(newSnapshot)),
								e.getValue().getRawOperatorState(),
								e.getValue().getManagedKeyedState(),
								e.getValue().getRawKeyedState()));
			}
		}

		savepoint = StateMetadataUtils.createNewSavepoint(savepoint, transformedOperatorState);
	}

	public <K, V> DataSet<KeyedStateRow> createKeyedStateRows(String uid, String stateName,
			DataSet<Tuple2<K, V>> newState) {
		return createKeyedStateRows(StateMetadataUtils.getOperatorState(savepoint, uid), stateName, newState);
	}

	public static <K, V> DataSet<KeyedStateRow> createKeyedStateRows(OperatorState oldState, String stateName,
			DataSet<Tuple2<K, V>> newState) {

		SerializableSupplier<KeyedBackendSerializationProxy<?>> proxySupplier = StateMetadataUtils
				.getKeyedBackendSerializationProxySupplier(oldState);

		return newState
				.map(new MapToKeyedStateRow<>(stateName, proxySupplier.get(), oldState.getMaxParallelism()));
	}

	public void replaceKeyedState(String uid, DataSet<KeyedStateRow> states) throws Exception {
		replaceKeyedState(StateMetadataUtils.getOperatorState(savepoint, uid), states);
	}

	public void replaceKeyedState(OperatorState oldState, DataSet<KeyedStateRow> states)
			throws Exception {

		int maxParallelism = oldState.getMaxParallelism();
		int parallelism = oldState.getParallelism();

		Path outDir = makeOutputDir(oldState.getOperatorID());

		DataSet<Tuple2<Integer, KeyedStateHandle>> handles = states
				.groupBy(new OperatorIndexForKeyGroupKey(maxParallelism, parallelism))
				.sortGroup(new KeyGroupAndStateIdKey(maxParallelism), Order.ASCENDING)
				.reduceGroup(new KeyedStateWriterFunction(maxParallelism, parallelism, StateMetadataUtils
						.getKeyedBackendSerializationProxySupplier(oldState), outDir));

		Map<Integer, KeyedStateHandle> handleMap = handles.collect().stream()
				.collect(Collectors.toMap(t -> t.f0, t -> t.f1));

		// We construct a new operatorstate with the collected handles
		OperatorState newOperatorState = new OperatorState(oldState.getOperatorID(), parallelism, maxParallelism);

		// Fill with the subtaskstates based on the old one (we need to preserve the
		// other states)
		oldState.getSubtaskStates().forEach((subtaskId, subtaskState) -> {
			KeyedStateHandle newHandle = handleMap.get(subtaskId);
			newOperatorState.putState(subtaskId,
					new OperatorSubtaskState(subtaskState.getManagedOperatorState(),
							subtaskState.getRawOperatorState(),
							new StateObjectCollection<>(
									newHandle != null ? Lists.newArrayList(newHandle) : Collections.emptyList()),
							subtaskState.getRawKeyedState()));
		});

		savepoint = StateMetadataUtils.createNewSavepoint(savepoint, newOperatorState);
	}

	private Path makeOutputDir(final OperatorID operatorId) {
		final Path outDir = new Path(new Path(newCheckpointBasePath, "mchk-" + checkpointId), "op-" + operatorId);
		try {
			outDir.getFileSystem().mkdirs(outDir);
		} catch (IOException ignore) {}
		return outDir;
	}

}
