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

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

import com.king.bravo.api.KeyedStateRow;

public class KeyGroupAndStateIdKey
		implements KeySelector<KeyedStateRow, Tuple2<Integer, Short>> {

	private static final long serialVersionUID = 1L;
	private int maxParallelism;

	public KeyGroupAndStateIdKey(int maxParallelism) {
		this.maxParallelism = maxParallelism;
	}

	@Override
	public Tuple2<Integer, Short> getKey(KeyedStateRow row) throws Exception {
		return row.getKeyGroupAndStateId(maxParallelism);
	}
}