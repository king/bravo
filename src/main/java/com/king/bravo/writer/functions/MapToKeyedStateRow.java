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
package com.king.bravo.writer.functions;

import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.CommonSerializerKeys;
import org.apache.flink.util.InstantiationUtil;

import com.king.bravo.api.KeyedStateRow;
import com.king.bravo.utils.StateMetadataUtils;
import com.king.bravo.utils.RocksDBKeySerializationUtils;

public class MapToKeyedStateRow<K, V> implements MapFunction<Tuple2<K, V>, KeyedStateRow> {

	private static final long serialVersionUID = 1L;
	private final int maxParallelism;
	private final TypeSerializer<K> keySerializer;
	private final TypeSerializer<V> valueSerializer;
	private final short stateId;
	private final int keygroupPrefixBytes;

	@SuppressWarnings("unchecked")
	public MapToKeyedStateRow(String string, KeyedBackendSerializationProxy<?> serializationProxy, int maxParallelism) {
		this.maxParallelism = maxParallelism;
		keygroupPrefixBytes = StateMetadataUtils.getKeyGroupPrefixBytes(maxParallelism);
		keySerializer = (TypeSerializer<K>) serializationProxy.getKeySerializer();

		short stateId = 0;
		List<StateMetaInfoSnapshot> stateMetaInfoSnapshots = serializationProxy
				.getStateMetaInfoSnapshots();

		for (StateMetaInfoSnapshot snapshot : stateMetaInfoSnapshots) {
			if (snapshot.getName().equals(string)) {
				if (!(snapshot.getTypeSerializer(
						CommonSerializerKeys.NAMESPACE_SERIALIZER) instanceof VoidNamespaceSerializer)) {
					throw new RuntimeException("Only operators states without namespaces are supported at the moment");
				}
				valueSerializer = (TypeSerializer<V>) snapshot.getTypeSerializer(CommonSerializerKeys.VALUE_SERIALIZER);
				this.stateId = stateId;
				return;
			}
			stateId++;
		}

		throw new RuntimeException("Could not find any keyed state with name " + string);
	}

	@Override
	public KeyedStateRow map(Tuple2<K, V> t) throws Exception {
		int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(t.f0, maxParallelism);
		ByteArrayOutputStreamWithPos os = new ByteArrayOutputStreamWithPos();
		DataOutputViewStreamWrapper ov = new DataOutputViewStreamWrapper(os);

		RocksDBKeySerializationUtils.writeKeyGroup(keyGroup, keygroupPrefixBytes, ov);
		RocksDBKeySerializationUtils.writeKey(t.f0, keySerializer, os, ov, false);
		RocksDBKeySerializationUtils.writeNameSpace(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, os,
				ov, false);

		os.close();
		return new KeyedStateRow(stateId, os.toByteArray(),
				InstantiationUtil.serializeToByteArray(valueSerializer, t.f1));
	}
}