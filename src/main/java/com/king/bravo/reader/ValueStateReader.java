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

import java.util.List;

import org.apache.commons.lang3.Validate;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.CommonSerializerKeys;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.king.bravo.api.KeyedStateRow;
import com.king.bravo.utils.RocksDBKeySerializationUtils;
import com.king.bravo.utils.StateMetadataUtils;

public abstract class ValueStateReader<K, V, O> extends RichFlatMapFunction<KeyedStateRow, O>
		implements ResultTypeQueryable<O> {

	private static final Logger LOGGER = LoggerFactory.getLogger(ValueStateReader.class);
	private static final long serialVersionUID = 1L;

	protected final String stateName;

	protected TypeSerializer<K> keyDeserializer;
	protected TypeSerializer<V> valueDeserializer;

	private final TypeInformation<K> keyType;
	private final TypeInformation<V> valueType;

	protected int keygroupPrefixBytes;

	protected boolean initialized = false;
	protected boolean outputTypesForDeserialization = true;

	private TypeInformation<O> outType;

	protected ValueStateReader(String stateName, TypeInformation<K> outKeyType, TypeInformation<V> outValueType,
			TypeInformation<O> outType) {
		this.stateName = stateName;
		this.outType = outType;
		this.valueType = outValueType;
		this.keyType = outKeyType;
	}

	@Override
	public TypeInformation<O> getProducedType() {
		if (!initialized) {
			throw new RuntimeException("Parser not initialized, use it with KeyedStateReader#parseKeyedStateRows");
		}
		return outType;
	}

	@SuppressWarnings("unchecked")
	public void init(OperatorState opState) {

		KeyedBackendSerializationProxy<?> serializationProxy = StateMetadataUtils
				.getKeyedBackendSerializationProxySupplier(opState).get();

		keygroupPrefixBytes = StateMetadataUtils.getKeyGroupPrefixBytes(opState.getMaxParallelism());

		if (keyDeserializer == null && !outputTypesForDeserialization) {
			keyDeserializer = (TypeSerializer<K>) serializationProxy.getKeySerializer();
		}

		List<StateMetaInfoSnapshot> stateMetaInfoSnapshots = serializationProxy.getStateMetaInfoSnapshots();

		for (StateMetaInfoSnapshot snapshot : stateMetaInfoSnapshots) {
			if (snapshot.getName().equals(stateName)) {
				if (valueDeserializer == null) {
					valueDeserializer = (TypeSerializer<V>) snapshot
							.getTypeSerializer(CommonSerializerKeys.VALUE_SERIALIZER);
				}
				this.initialized = true;
				return;
			}
		}

		throw new IllegalArgumentException("Could not find any keyed state with name " + stateName);
	}

	public ValueStateReader<K, V, O> withOutputTypesForDeserialization() {
		outputTypesForDeserialization = true;
		keyDeserializer = null;
		valueDeserializer = null;
		return this;
	}

	public ValueStateReader<K, V, O> withKeyDeserializer(TypeSerializer<K> keyDeserializer) {
		this.keyDeserializer = Validate.notNull(keyDeserializer);
		return this;
	}

	public ValueStateReader<K, V, O> withValueDeserializer(TypeSerializer<V> valueDeserializer) {
		this.valueDeserializer = Validate.notNull(valueDeserializer);
		return this;
	}

	@Override
	public void open(Configuration c) {
		ExecutionConfig executionConfig = getRuntimeContext().getExecutionConfig();
		if (keyDeserializer == null && keyType != null) {
			keyDeserializer = keyType.createSerializer(executionConfig);
		}
		if (valueDeserializer == null && valueType != null) {
			valueDeserializer = valueType.createSerializer(executionConfig);
		}

		LOGGER.info(
				"Initialized KeyedStateRowParser: keyDeserializer: {} valueDeserializer: {} outKeyType: {} outValueType: {}",
				keyDeserializer, valueDeserializer, keyType, valueType);
	}

	/**
	 * Create a reader for reading the state key-value pairs for the given value
	 * state name. The provided type info will be used to deserialize the state
	 * (allowing possible optimizations)
	 */
	public static <K, V> ValueStateReader<K, V, Tuple2<K, V>> forStateKVPairs(String stateName,
			TypeInformation<K> outKeyType,
			TypeInformation<V> outValueType) {
		return new TupleParser<>(stateName, outKeyType, outValueType);
	}

	/**
	 * Create a reader for reading the state key-value pairs for the given value
	 * state name. The provided type info will be used to deserialize the state
	 * (allowing possible optimizations)
	 */
	public static <K, V> ValueStateReader<K, V, Tuple2<K, V>> forStateKVPairs(String stateName,
			TypeHint<Tuple2<K, V>> tupleTypeHint) {
		TupleTypeInfo<Tuple2<K, V>> tupleType = (TupleTypeInfo<Tuple2<K, V>>) tupleTypeHint.getTypeInfo();
		return forStateKVPairs(stateName, tupleType.getTypeAt(0), tupleType.getTypeAt(1));
	}

	/**
	 * Create a reader for reading the state values for the given value state name.
	 * The provided type info will be used to deserialize the state (allowing
	 * possible optimizations)
	 */
	public static <K, V> ValueStateReader<K, V, V> forStateValues(String stateName, TypeInformation<V> outValueType) {
		return new ValueParser<>(stateName, outValueType);
	}

	/**
	 * Create a reader for reading the state values for the given value state name.
	 * The provided type info will be used to deserialize the state (allowing
	 * possible optimizations)
	 */
	public static <K, V> ValueStateReader<K, V, V> forStateValues(String stateName, TypeHint<V> outValueTypeHint) {
		return forStateValues(stateName, outValueTypeHint.getTypeInfo());
	}

	public String getStateName() {
		return stateName;
	}

	public static class TupleParser<K, V> extends ValueStateReader<K, V, Tuple2<K, V>> {

		private static final long serialVersionUID = 1L;

		@SuppressWarnings({ "rawtypes", "unchecked" })
		public TupleParser(String stateName, TypeInformation<K> outKeyType, TypeInformation<V> outValueType) {
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
				key = RocksDBKeySerializationUtils.readKey(keyDeserializer, keyIs, iw, false);
			}

			V value = InstantiationUtil.deserializeFromByteArray(valueDeserializer, valueBytes);
			out.collect(Tuple2.of(key, value));
		}
	}

	public static class ValueParser<K, V> extends ValueStateReader<K, V, V> {

		private static final long serialVersionUID = 1L;

		public ValueParser(String stateName, TypeInformation<V> outValueType) {
			super(stateName, null, outValueType, outValueType);
		}

		@Override
		public void flatMap(KeyedStateRow row, Collector<V> out) throws Exception {
			if (!stateName.equals(row.getStateName())) {
				return;
			}

			out.collect(InstantiationUtil.deserializeFromByteArray(valueDeserializer, row.getValueBytes()));
		}
	}
}