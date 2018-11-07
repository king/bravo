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

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang3.Validate;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.king.bravo.types.KeyedStateRow;
import com.king.bravo.utils.StateMetadataUtils;

public abstract class KeyedStateReader<K, V, O> extends RichFlatMapFunction<KeyedStateRow, O>
		implements ResultTypeQueryable<O> {

	private static final Logger LOGGER = LoggerFactory.getLogger(KeyedStateReader.class);
	private static final long serialVersionUID = 1L;

	protected final String stateName;

	protected TypeSerializer<K> keyDeserializer;
	protected TypeSerializer<V> valueDeserializer;
	protected TypeInformation<K> keyDeserializerType;
	protected TypeInformation<V> valueDeserializerType;

	private final TypeInformation<K> outKeyType;
	private final TypeInformation<V> outValueType;
	private final TypeInformation<O> outType;

	protected int keygroupPrefixBytes;

	protected boolean initialized = false;

	protected boolean ttlState = false;

	protected KeyedStateReader(String stateName, TypeInformation<K> keyType, TypeInformation<V> valueType,
			TypeInformation<O> outType) {
		this.stateName = stateName;
		this.outType = outType;
		this.outValueType = valueType;
		this.outKeyType = keyType;
	}

	@Override
	public TypeInformation<O> getProducedType() {
		if (!initialized) {
			throw new RuntimeException("Parser not initialized, use it with KeyedStateReader#parseKeyedStateRows");
		}
		return outType;
	}

	@SuppressWarnings("unchecked")
	public void configure(int maxParallelism, TypeSerializer<?> keySerializer, TypeSerializer<?> valueSerializer)
			throws Exception {

		keygroupPrefixBytes = StateMetadataUtils.getKeyGroupPrefixBytes(maxParallelism);

		if (this.keyDeserializer == null && this.keyDeserializerType == null) {
			this.keyDeserializer = (TypeSerializer<K>) keySerializer;
		}

		boolean ttlSerializer = StateMetadataUtils.isTtlState(valueSerializer);
		if (ttlSerializer) {
			ttlState = true;
		}

		if (this.valueDeserializer == null && this.valueDeserializerType == null) {
			this.valueDeserializer = ttlSerializer ? StateMetadataUtils.unwrapTtlSerializer(valueSerializer)
					: (TypeSerializer<V>) valueSerializer;
		}
		initialized = true;
	}

	public KeyedStateReader<K, V, O> withOutputTypesForDeserialization() {
		withKeyDeserializer(outKeyType);
		withValueDeserializer(outValueType);
		return this;
	}

	public KeyedStateReader<K, V, O> withKeyDeserializer(TypeInformation<K> keyDeserializer) {
		this.keyDeserializerType = Validate.notNull(keyDeserializer);
		return this;
	}

	public KeyedStateReader<K, V, O> withValueDeserializer(TypeInformation<V> valueDeserializer) {
		this.valueDeserializerType = Validate.notNull(valueDeserializer);
		return this;
	}

	public KeyedStateReader<K, V, O> withKeyDeserializer(TypeSerializer<K> keyDeserializer) {
		this.keyDeserializer = Validate.notNull(keyDeserializer);
		return this;
	}

	public KeyedStateReader<K, V, O> withValueDeserializer(TypeSerializer<V> valueDeserializer) {
		this.valueDeserializer = Validate.notNull(valueDeserializer);
		return this;
	}

	@Override
	public void open(Configuration c) {
		ExecutionConfig executionConfig = getRuntimeContext().getExecutionConfig();

		if (keyDeserializer == null) {
			keyDeserializer = keyDeserializerType.createSerializer(executionConfig);
		}

		if (valueDeserializer == null) {
			valueDeserializer = valueDeserializerType.createSerializer(executionConfig);
		}

		LOGGER.info("Initialized KeyedStateReader: keyDeserializer: {} valueDeserializer: {}", keyDeserializer,
				valueDeserializer);
	}

	/**
	 * Create a reader for reading the state key-value pairs for the given value
	 * state name. The provided type info will be used to deserialize the state
	 * (allowing possible optimizations)
	 */
	public static <K, V> KeyedStateReader<K, V, Tuple2<K, V>> forValueStateKVPairs(String stateName,
			TypeInformation<K> outKeyType,
			TypeInformation<V> outValueType) {
		return new ValueStateKVReader<>(stateName, outKeyType, outValueType);
	}

	/**
	 * Create a reader for reading the state key-value pairs for the given value
	 * state name. The provided type info will be used to deserialize the state
	 * (allowing possible optimizations)
	 */
	public static <K, V> KeyedStateReader<K, V, Tuple2<K, V>> forValueStateKVPairs(String stateName,
			TypeHint<Tuple2<K, V>> tupleTypeHint) {
		TupleTypeInfo<Tuple2<K, V>> tupleType = (TupleTypeInfo<Tuple2<K, V>>) tupleTypeHint.getTypeInfo();
		return forValueStateKVPairs(stateName, tupleType.getTypeAt(0), tupleType.getTypeAt(1));
	}

	/**
	 * Create a reader for reading the state values for the given value state
	 * name. The provided type info will be used to deserialize the state
	 * (allowing possible optimizations)
	 */
	public static <K, V> KeyedStateReader<K, V, V> forValueStateValues(String stateName,
			TypeInformation<V> outValueType) {
		return new ValueStateValueReader<>(stateName, outValueType, false);
	}

	/**
	 * Create a reader for reading the state values for the given map state
	 * name. The provided type info will be used to deserialize the state
	 * (allowing possible optimizations)
	 */
	public static <K, V> KeyedStateReader<K, V, V> forMapStateValues(String stateName,
			TypeInformation<V> outValueType) {
		return forMapStateValues(stateName, outValueType, false);
	}

	/**
	 * Create a reader for reading the state values for the given map state
	 * name. The provided type info will be used to deserialize the state
	 * (allowing possible optimizations)
	 */
	public static <K, V> KeyedStateReader<K, V, V> forMapStateValues(String stateName,
			TypeInformation<V> outValueType, boolean ttlState) {
		KeyedStateReader<K, V, V> reader = new ValueStateValueReader<K, V>(stateName, outValueType, true)
				.withValueDeserializer(outValueType);
		reader.ttlState = ttlState;
		return reader;
	}

	/**
	 * Create a reader for reading the state values for the given list state.
	 * The provided type info will be used to deserialize the state (allowing
	 * possible optimizations)
	 */
	public static <K, V> KeyedStateReader<K, V, Tuple2<K, V>> forListStateValues(String stateName,
			TypeInformation<K> outKeyType, TypeInformation<V> outValueType) {
		return new ListStateFlattenReader<>(stateName, outKeyType, outValueType);
	}

	/**
	 * Create a reader for reading the state values for the given list state.
	 * The provided type info will be used to deserialize the state (allowing
	 * possible optimizations)
	 */
	public static <K, V> KeyedStateReader<K, V, Tuple2<K, List<V>>> forListStates(String stateName,
			TypeInformation<K> outKeyType, TypeInformation<V> outValueType) {
		return new ListStateListReader<>(stateName, outKeyType, outValueType);
	}

	/**
	 * Create a reader for reading the state key-mapkey-value triplets for the
	 * given map state name. The provided type info will be used to deserialize
	 * the state (allowing possible optimizations)
	 */
	public static <K, MK, V> KeyedStateReader<K, V, Tuple3<K, MK, V>> forMapStateEntries(String stateName,
			TypeInformation<K> outKeyType, TypeInformation<MK> outMapKeyType, TypeInformation<V> outValueType) {
		return forMapStateEntries(stateName, outKeyType, outMapKeyType, outValueType, false);
	}

	/**
	 * Create a reader for reading the state key-mapkey-value triplets for the
	 * given map state name. The provided type info will be used to deserialize
	 * the state (allowing possible optimizations)
	 */
	public static <K, MK, V> KeyedStateReader<K, V, Tuple3<K, MK, V>> forMapStateEntries(String stateName,
			TypeInformation<K> outKeyType, TypeInformation<MK> outMapKeyType, TypeInformation<V> outValueType,
			boolean ttlState) {

		MapStateKKVReader<K, MK, V> reader = new MapStateKKVReader<>(stateName, outKeyType, outMapKeyType,
				outValueType);
		reader.ttlState = ttlState;
		return reader;
	}

	public String getStateName() {
		return stateName;
	}

	protected void skipTimestampIfTtlEnabled(DataInputViewStreamWrapper iw) throws IOException {
		if (ttlState) {
			iw.skipBytesToRead(Long.BYTES);
		}
	}
}
