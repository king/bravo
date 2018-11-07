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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.MiniClusterResource;
import org.apache.flink.test.util.MiniClusterResourceConfiguration;

public class MiniClusterResourceFactory {
	private final int numTaskManagers;
	private final int numSlotsPerTaskManager;
	private final Configuration config;

	public MiniClusterResourceFactory(int numTaskManagers, int numSlotsPerTaskManager, Configuration config) {
		this.numTaskManagers = numTaskManagers;
		this.numSlotsPerTaskManager = numSlotsPerTaskManager;
		this.config = config;
	}

	public MiniClusterResource get() {
		return new MiniClusterResource(
				new MiniClusterResourceConfiguration.Builder()
						.setConfiguration(config)
						.setNumberTaskManagers(numTaskManagers)
						.setNumberSlotsPerTaskManager(numSlotsPerTaskManager)
						.build());
	}
}
