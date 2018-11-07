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
package com.king.bravo.testing;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import com.google.common.collect.Lists;

public class CollectingSink implements SinkFunction<String>, ListCheckpointed<Integer> {
	private static final long serialVersionUID = 1L;

	public static List<String> OUTPUT = new ArrayList<>();

	public CollectingSink() {}

	@Override
	public void invoke(String out) throws Exception {
		OUTPUT.add(out);
	}

	@Override
	public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
		return Lists.newArrayList(OUTPUT.size());
	}

	@Override
	public void restoreState(List<Integer> state) throws Exception {
		int lastValid = state.get(0);
		List<String> newOutput = new ArrayList<>();
		for (int i = 0; i < lastValid; i++) {
			newOutput.add(OUTPUT.get(i));
		}
		OUTPUT = (List<String>) newOutput;
	}

}
