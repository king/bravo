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
package com.king.bravo.testing.actions;

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;

import com.king.bravo.testing.PipelineAction;

public class Process implements PipelineAction {

	final String output;
	final long timestamp;

	public Process(String output, long timestamp) {
		this.output = output;
		this.timestamp = timestamp;
	}

	@Override
	public void withCheckpointLock(SourceContext<String> ctx) throws Exception {
		ctx.collectWithTimestamp(output, timestamp);
	}
}
