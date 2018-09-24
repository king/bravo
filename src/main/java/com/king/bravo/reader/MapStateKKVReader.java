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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.util.Collector;

import com.king.bravo.types.KeyedStateRow;
import com.king.bravo.utils.RocksDBKeySerializationUtils;

public class MapStateKKVReader<K, MK, V> extends KeyedStateReader<K, V, Tuple3<K, MK, V>> {

	private static final long serialVersionUID = 1L;
	private TypeInformation<MK> mapKeytype;
	private TypeSerializer<MK> mapKeySerializer;

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public MapStateKKVReader(String stateName, TypeInformation<K> outKeyType, TypeInformation<MK> mapKeytype,
			TypeInformation<V> outValueType) {
		super(stateName, outKeyType, outValueType,
				new TupleTypeInfo(Tuple3.class, outKeyType, mapKeytype, outValueType));
		this.mapKeytype = mapKeytype;
	}

	@Override
	public void flatMap(KeyedStateRow row, Collector<Tuple3<K, MK, V>> out) throws Exception {
		if (!stateName.equals(row.getStateName())) {
			return;
		}

		byte[] keyAndNamespaceBytes = row.getKeyAndNamespaceBytes();
		byte[] valueBytes = row.getValueBytes();

		K key;
		MK mapKey;
		try (ByteArrayInputStreamWithPos keyIs = new ByteArrayInputStreamWithPos(keyAndNamespaceBytes)) {
			DataInputViewStreamWrapper iw = new DataInputViewStreamWrapper(keyIs);
			iw.skipBytesToRead(keygroupPrefixBytes);
			key = RocksDBKeySerializationUtils.readKey(keyDeserializer, keyIs, iw, false);
			VoidNamespaceSerializer.INSTANCE.deserialize(iw);
			mapKey = mapKeySerializer.deserialize(iw);
		}

		V value = null;
		try (ByteArrayInputStreamWithPos valIs = new ByteArrayInputStreamWithPos(valueBytes)) {
			DataInputViewStreamWrapper iw = new DataInputViewStreamWrapper(valIs);
			if (!iw.readBoolean()) {
				value = valueDeserializer.deserialize(iw);
			}
		}
		if (value == null) {
			throw new RuntimeException("MapStates with null values are not supported at the moment.");
		}
		out.collect(Tuple3.of(key, mapKey, value));
	}

	@Override
	public void open(Configuration c) {
		super.open(c);
		mapKeySerializer = mapKeytype.createSerializer(getRuntimeContext().getExecutionConfig());
	}
}
