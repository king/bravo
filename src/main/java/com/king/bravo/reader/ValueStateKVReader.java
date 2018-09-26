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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;

import com.king.bravo.types.KeyedStateRow;
import com.king.bravo.utils.RocksDBUtils;

public class ValueStateKVReader<K, V> extends KeyedStateReader<K, V, Tuple2<K, V>> {

	private static final long serialVersionUID = 1L;

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public ValueStateKVReader(String stateName, TypeInformation<K> outKeyType, TypeInformation<V> outValueType) {
		super(stateName, outKeyType, outValueType, new TupleTypeInfo(Tuple2.class, outKeyType, outValueType));
	}

	@Override
	public void flatMap(KeyedStateRow row, Collector<Tuple2<K, V>> out) throws Exception {
		if (!stateName.equals(row.getStateName())) {
			return;
		}

		byte[] keyAndNamespaceBytes = row.getKeyAndNamespaceBytes();
		byte[] valueBytes = row.getValueBytes();

		K key;
		try (ByteArrayInputStreamWithPos keyIs = new ByteArrayInputStreamWithPos(keyAndNamespaceBytes)) {
			DataInputViewStreamWrapper iw = new DataInputViewStreamWrapper(keyIs);
			iw.skipBytesToRead(keygroupPrefixBytes);
			key = RocksDBUtils.readKey(keyDeserializer, keyIs, iw, false);
		}

		V value = InstantiationUtil.deserializeFromByteArray(valueDeserializer, valueBytes);
		out.collect(Tuple2.of(key, value));
	}
}
