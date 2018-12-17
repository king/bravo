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
package com.king.bravo.utils;

import org.apache.flink.api.common.typeutils.CompositeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointV2;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnappyStreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorage;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.CommonSerializerKeys;

import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class StateMetadataUtils {

	/**
	 * Load the Savepoint metadata object from the given path
	 */
	public static Savepoint loadSavepoint(String checkpointPointer) throws IOException {
		try {
			Method resolveCheckpointPointer = AbstractFsCheckpointStorage.class.getDeclaredMethod(
					"resolveCheckpointPointer",
					String.class);
			resolveCheckpointPointer.setAccessible(true);
			CompletedCheckpointStorageLocation loc = (CompletedCheckpointStorageLocation) resolveCheckpointPointer
					.invoke(null, checkpointPointer);

			return Checkpoints.loadCheckpointMetadata(new DataInputStream(loc.getMetadataHandle().openInputStream()),
					StateMetadataUtils.class.getClassLoader());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	public static OperatorState getOperatorState(Savepoint savepoint, String uid) {
		return getOperatorState(savepoint, Identifiers.operatorId(uid));
	}

	public static OperatorState getOperatorState(Savepoint savepoint, OperatorID opId) {
		return savepoint
				.getOperatorStates()
				.stream()
				.filter(os -> os.getOperatorID().equals(opId))
				.findAny()
				.orElseThrow(() -> new RuntimeException("No operator state with id " + opId.toString()));
	}

	public static int getKeyGroupPrefixBytes(int maxParallelism) {
		return maxParallelism > (Byte.MAX_VALUE + 1) ? 2 : 1;
	}

	/**
	 * Create a new {@link Savepoint} by replacing certain
	 * {@link OperatorState}s of an old {@link Savepoint}
	 * 
	 * @param oldSavepoint
	 *            {@link Savepoint} to base the new state on
	 * @param statesToReplace
	 *            States that will be replaced, all else will be kept
	 * @return A new valid {@link Savepoint} metadata object.
	 */
	public static Savepoint createNewSavepoint(Savepoint oldSavepoint, OperatorState... statesToReplace) {
		return createNewSavepoint(oldSavepoint, Arrays.asList(statesToReplace));
	}

	/**
	 * Create a new {@link Savepoint} by replacing certain
	 * {@link OperatorState}s of an old {@link Savepoint}
	 * 
	 * @param oldSavepoint
	 *            {@link Savepoint} to base the new state on
	 * @param statesToReplace
	 *            States that will be replaced, all else will be kept
	 * @return A new valid {@link Savepoint} metadata object.
	 */
	public static Savepoint createNewSavepoint(Savepoint oldSavepoint, Collection<OperatorState> statesToReplace) {

		Map<OperatorID, OperatorState> newStates = oldSavepoint.getOperatorStates().stream()
				.collect(Collectors.toMap(OperatorState::getOperatorID, o -> o));

		statesToReplace.forEach(os -> newStates.put(os.getOperatorID(), os));

		return new SavepointV2(oldSavepoint.getCheckpointId(), newStates.values(), oldSavepoint.getMasterStates());
	}

	public static Optional<KeyedBackendSerializationProxy<?>> getKeyedBackendSerializationProxy(OperatorState opState) {
		try {
			KeyedStateHandle firstHandle = opState.getStates().iterator().next().getManagedKeyedState().iterator()
					.next();
			if (firstHandle instanceof IncrementalKeyedStateHandle) {
				return Optional.of(getKeyedBackendSerializationProxy(
						((IncrementalKeyedStateHandle) firstHandle).getMetaStateHandle()));
			} else {
				return Optional.of(getKeyedBackendSerializationProxy((StreamStateHandle) firstHandle));
			}
		} catch (Exception e) {
			return Optional.empty();
		}
	}

	public static StreamCompressionDecorator getCompressionDecorator(KeyedBackendSerializationProxy<?> proxy) {
		return proxy.isUsingKeyGroupCompression()
				? SnappyStreamCompressionDecorator.INSTANCE
				: UncompressedStreamCompressionDecorator.INSTANCE;
	}

	@SuppressWarnings("unchecked")
	public static <T> Optional<TypeSerializer<T>> getSerializer(KeyedBackendSerializationProxy<?> proxy,
			String stateName) {

		for (StateMetaInfoSnapshot snapshot : proxy.getStateMetaInfoSnapshots()) {
			if (snapshot.getName().equals(stateName)) {
				return Optional
						.of((TypeSerializer<T>) snapshot
								.getTypeSerializerConfigSnapshot(CommonSerializerKeys.VALUE_SERIALIZER)
								.restoreSerializer());
			}
		}

		return Optional.empty();
	}

	public static Map<Integer, String> getStateIdMapping(KeyedBackendSerializationProxy<?> proxy) {
		Map<Integer, String> stateIdMapping = new HashMap<>();

		int stateId = 0;
		for (StateMetaInfoSnapshot snapshot : proxy.getStateMetaInfoSnapshots()) {
			stateIdMapping.put(stateId, snapshot.getName());
			stateId++;
		}

		return stateIdMapping;
	}

	public static KeyedBackendSerializationProxy<?> getKeyedBackendSerializationProxy(
			StreamStateHandle streamStateHandle) {
		KeyedBackendSerializationProxy<Integer> serializationProxy = new KeyedBackendSerializationProxy<>(
				StateMetadataUtils.class.getClassLoader());
		try (FSDataInputStream is = streamStateHandle.openInputStream()) {
			DataInputViewStreamWrapper iw = new DataInputViewStreamWrapper(is);
			serializationProxy.read(iw);
			return serializationProxy;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static Path writeSavepointMetadata(Path newCheckpointBasePath, Savepoint savepoint) throws IOException {
		Path p = new Path(newCheckpointBasePath, AbstractFsCheckpointStorage.METADATA_FILE_NAME);
		Checkpoints.storeCheckpointMetadata(savepoint,
				newCheckpointBasePath.getFileSystem().create(p, WriteMode.NO_OVERWRITE));
		return p;
	}

	public static boolean isTtlState(TypeSerializer<?> valueSerializer) {
		boolean ttlSerializer = valueSerializer.getClass().getName()
				.startsWith("org.apache.flink.runtime.state.ttl.TtlStateFactory$TtlSerializer");
		return ttlSerializer;
	}

	public static <T> TypeSerializer<T> unwrapTtlSerializer(TypeSerializer<?> valueSerializer) throws Exception {
		Field f = CompositeSerializer.class.getDeclaredField("fieldSerializers");
		f.setAccessible(true);
		return (TypeSerializer<T>) ((TypeSerializer<Object>[]) f.get(valueSerializer))[1];
	}

}
