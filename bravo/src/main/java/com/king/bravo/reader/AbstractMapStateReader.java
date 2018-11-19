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
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.configuration.Configuration;

public abstract class AbstractMapStateReader<K, V, O> extends KeyedStateReader<K, V, O> {

	private static final long serialVersionUID = 1L;
	protected TypeInformation<?> mapKeytype = null;
	protected TypeSerializer<?> mapKeySerializer = null;

	protected AbstractMapStateReader(String stateName, TypeInformation<K> keyType, TypeInformation<V> valueType,
			TypeInformation<O> outType) {
		super(stateName, keyType, valueType, outType);
	}

	public KeyedStateReader<K, V, O> withMapKeyDeserializer(TypeInformation<?> type) {
		this.mapKeytype = type;
		return this;
	}

	public KeyedStateReader<K, V, O> withMapKeyDeserializer(TypeSerializer<?> serializer) {
		this.mapKeySerializer = serializer;
		return this;
	}

	@SuppressWarnings("unchecked")
	public void configure(int maxParallelism, TypeSerializer<?> keySerializer, TypeSerializer<?> valueSerializer)
			throws Exception {

		if (valueSerializer instanceof MapSerializer) {
			MapSerializer<?, ?> mapSerializer = (MapSerializer<?, ?>) valueSerializer;
			if (mapKeytype == null && mapKeySerializer == null) {
				mapKeySerializer = (TypeSerializer<Object>) mapSerializer.getKeySerializer();
			}

			super.configure(maxParallelism, keySerializer, mapSerializer.getValueSerializer());
        } else if (valueSerializer instanceof KryoSerializer) {
            KryoSerializer<?> kryoSerializer = (KryoSerializer<?>) valueSerializer;
            if (mapKeytype == null && mapKeySerializer == null) {
                mapKeySerializer = (TypeSerializer<Object>) kryoSerializer;
            }

            super.configure(maxParallelism, keySerializer, kryoSerializer);
		} else {
			throw new RuntimeException("Doesnt seem like a map state");
		}
	}

	@Override
	public void open(Configuration c) {
		super.open(c);
		if (mapKeySerializer == null) {
			mapKeySerializer = mapKeytype.createSerializer(getRuntimeContext().getExecutionConfig());
		}
	}
}
