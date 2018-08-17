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

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointV2;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.OperatorBackendSerializationProxy;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.CommonSerializerKeys;

public class StateMetadataUtils {

	/**
	 * Load the Savepoint metadata object from the given path
	 */
	public static Savepoint loadSavepoint(Path path) throws IOException {
		Path metaPath = path.getName().equals("_metadata") ? path : new Path(path, "_metadata");
		return Checkpoints.loadCheckpointMetadata(new DataInputStream(metaPath.getFileSystem().open(metaPath)),
				StateMetadataUtils.class.getClassLoader());
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

	public static SerializableSupplier<OperatorBackendSerializationProxy> getOperatorBackendSerializationProxySupplier(
			OperatorState opState) {

		OperatorStateHandle opStateHandle = opState.getState(0).getManagedOperatorState().iterator().next();
		return () -> {
			OperatorBackendSerializationProxy serializationProxy = new OperatorBackendSerializationProxy(
					StateMetadataUtils.class.getClassLoader());
			try (FSDataInputStream is = opStateHandle.openInputStream()) {
				DataInputViewStreamWrapper iw = new DataInputViewStreamWrapper(is);
				serializationProxy.read(iw);
				return serializationProxy;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		};
	}

	public static int getKeyedStateId(OperatorState state, String stateName) {
		int stateId = 0;
		List<StateMetaInfoSnapshot> stateMetaInfoSnapshots = StateMetadataUtils
				.getKeyedBackendSerializationProxySupplier(state)
				.get().getStateMetaInfoSnapshots();

		for (StateMetaInfoSnapshot snapshot : stateMetaInfoSnapshots) {
			if (snapshot.getName().equals(stateName)) {
				if (!(snapshot.getTypeSerializer(
						CommonSerializerKeys.NAMESPACE_SERIALIZER) instanceof VoidNamespaceSerializer)) {
					throw new RuntimeException("Only operators states without namespaces are supported at the moment");
				}
				return stateId;
			}
			stateId++;
		}

		throw new RuntimeException("Could not find any keyed state with name " + stateName);
	}

	public static Map<String, Integer> getKeyedStateIds(OperatorState state, Collection<String> stateNames) {
		int stateId = 0;
		List<StateMetaInfoSnapshot> stateMetaInfoSnapshots = StateMetadataUtils
				.getKeyedBackendSerializationProxySupplier(state)
				.get().getStateMetaInfoSnapshots();

		Map<String, Integer> out = new HashMap<>();

		for (StateMetaInfoSnapshot snapshot : stateMetaInfoSnapshots) {
			if (stateNames.contains(snapshot.getName())) {
				if (!(snapshot.getTypeSerializer(
						CommonSerializerKeys.NAMESPACE_SERIALIZER) instanceof VoidNamespaceSerializer)) {
					throw new RuntimeException("Only operators states without namespaces are supported at the moment");
				}
				out.put(snapshot.getName(), stateId);
			}
			stateId++;
		}

		stateNames.forEach(s -> {
			if (!out.containsKey(s)) {
				throw new RuntimeException("Could not find any keyed state with name " + s);
			}
		});

		return out;
	}

	public static int getKeyGroupPrefixBytes(int maxParallelism) {
		return maxParallelism > (Byte.MAX_VALUE + 1) ? 2 : 1;
	}

	public static Savepoint createNewSavepoint(Savepoint oldSavepoint, OperatorState... statesToReplace) {
		return createNewSavepoint(oldSavepoint, Arrays.asList(statesToReplace));
	}

	public static Savepoint createNewSavepoint(Savepoint oldSavepoint, Collection<OperatorState> statesToReplace) {

		Map<OperatorID, OperatorState> newStates = oldSavepoint.getOperatorStates().stream()
				.collect(Collectors.toMap(OperatorState::getOperatorID, o -> o));

		statesToReplace.forEach(os -> newStates.put(os.getOperatorID(), os));

		return new SavepointV2(oldSavepoint.getCheckpointId(), newStates.values(), oldSavepoint.getMasterStates());
	}

	public static SerializableSupplier<KeyedBackendSerializationProxy<?>> getKeyedBackendSerializationProxySupplier(
			OperatorState opState) {
		return () -> getKeyedBackendSerializationProxy(opState);
	}

	public static KeyedBackendSerializationProxy<?> getKeyedBackendSerializationProxy(OperatorState opState) {
		final KeyGroupsStateHandle keyedHandle = firstKeyGroupStateHandle(opState);
		return getKeyedBackendSerializationProxy(keyedHandle);
	}

	private static KeyGroupsStateHandle firstKeyGroupStateHandle(OperatorState opState) {
		return (KeyGroupsStateHandle) opState.getState(0).getManagedKeyedState().iterator().next();
	}

	private static KeyedBackendSerializationProxy<?> getKeyedBackendSerializationProxy(
			KeyGroupsStateHandle keyedHandle) {
		KeyedBackendSerializationProxy<Integer> serializationProxy = new KeyedBackendSerializationProxy<>(
				StateMetadataUtils.class.getClassLoader(), false);
		try (FSDataInputStream is = keyedHandle.openInputStream()) {
			DataInputViewStreamWrapper iw = new DataInputViewStreamWrapper(is);
			serializationProxy.read(iw);
			return serializationProxy;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
