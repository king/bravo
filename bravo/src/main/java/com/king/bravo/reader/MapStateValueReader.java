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
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.util.Collector;

import com.king.bravo.types.KeyedStateRow;

public class MapStateValueReader<K, V> extends AbstractMapStateReader<K, V, V> {

	private static final long serialVersionUID = 1L;

	public MapStateValueReader(String stateName, TypeInformation<V> outValueType) {
		super(stateName, null, outValueType, outValueType);
	}

	@Override
	public void flatMap(KeyedStateRow row, Collector<V> out) throws Exception {
		if (!stateName.equals(row.getStateName())) {
			return;
		}

		byte[] valueBytes = row.getValueBytes();

		V value = null;
		try (ByteArrayInputStreamWithPos valIs = new ByteArrayInputStreamWithPos(valueBytes)) {
			DataInputViewStreamWrapper iw = new DataInputViewStreamWrapper(valIs);
			if (!iw.readBoolean()) {
				skipTimestampIfTtlEnabled(iw);
				value = valueDeserializer.deserialize(iw);
			}
		}
		if (value == null) {
			throw new RuntimeException("MapStates with null values are not supported at the moment.");
		} else {
			out.collect(value);
		}
	}
}
