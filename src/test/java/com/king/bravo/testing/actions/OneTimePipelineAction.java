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

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;

import com.king.bravo.testing.PipelineAction;

public abstract class OneTimePipelineAction implements PipelineAction {

	private boolean checkpointLockTriggered = false;
	private boolean clusterActionTriggered = false;

	public final void withCheckpointLock(SourceContext<String> ctx) {
		if (checkpointLockTriggered) {
			return;
		}

		checkpointLockTriggered = true;
		onceWithCheckpointLock(ctx);
	}

	protected void onceWithCheckpointLock(SourceContext<String> ctx) {}

	public final void executeClusterAction(ClusterClient<?> client, JobID id) throws Exception {

		if (clusterActionTriggered) {
			return;
		}

		clusterActionTriggered = true;
		onceExecuteClusterAction(client, id);
	};

	protected void onceExecuteClusterAction(ClusterClient<?> client, JobID id) throws Exception {}

}
