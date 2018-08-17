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
package com.king.bravo.api;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

import com.king.bravo.utils.RocksDBKeySerializationUtils;

/**
 * Raw state row containing minimal data necessary for the RocksDB state backend
 * to write it back.
 *
 */
public class KeyedStateRow extends Tuple3<Short, byte[], byte[]> {

	private static final long serialVersionUID = 1L;

	public KeyedStateRow() {
		super();
	}

	public KeyedStateRow(short stateId, byte[] keyAndNamespaceBytes, byte[] valueBytes) {
		super(stateId, keyAndNamespaceBytes, valueBytes);
	}

	public Short getStateId() {
		return f0;
	}

	public byte[] getKeyAndNamespaceBytes() {
		return f1;
	}

	public byte[] getValueBytes() {
		return f2;
	}

	public int getKeyGroup(int maxParallelism) throws IOException {
		try (ByteArrayInputStream is = new ByteArrayInputStream(getKeyAndNamespaceBytes())) {
			return RocksDBKeySerializationUtils.readKeyGroup(1, new DataInputViewStreamWrapper(is));
		}
	}

	public Integer getOperatorIndex(int maxParallelism, int parallelism) throws Exception {
		return KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(maxParallelism, parallelism,
				getKeyGroup(maxParallelism));
	}

	public Tuple2<Integer, Short> getKeyGroupAndStateId(int maxParallelism) throws IOException {
		return Tuple2.of(getKeyGroup(maxParallelism), getStateId());
	}
}
