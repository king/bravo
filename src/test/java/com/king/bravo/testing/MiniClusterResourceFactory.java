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