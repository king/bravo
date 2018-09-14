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

import com.king.bravo.types.KeyedStateRow;

public class OperatorIndexForKeyGroupKey implements KeySelector<KeyedStateRow, Integer> {

	private static final long serialVersionUID = 1L;

	private final int maxParallelism;
	private final int parallelism;

	public OperatorIndexForKeyGroupKey(int maxParallelism, int parallelism) {
		this.maxParallelism = maxParallelism;
		this.parallelism = parallelism;
	}

	@Override
	public Integer getKey(KeyedStateRow state) throws Exception {
		return state.getOperatorIndex(maxParallelism, parallelism);
	}
}