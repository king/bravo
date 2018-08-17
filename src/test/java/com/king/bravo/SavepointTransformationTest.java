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
package com.king.bravo;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.time.Duration;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterResource;
import org.apache.flink.test.util.MiniClusterResourceConfiguration;
import org.apache.flink.util.TestLogger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.king.bravo.api.KeyedStateRow;
import com.king.bravo.reader.KeyedStateReader;
import com.king.bravo.reader.ValueStateReader;
import com.king.bravo.utils.StateMetadataUtils;
import com.king.bravo.writer.StateTransformer;

public class SavepointTransformationTest extends TestLogger {

	static final Logger LOGGER = LoggerFactory.getLogger(SavepointTransformationTest.class);

	final int numTaskManagers = 2;
	final int numSlotsPerTaskManager = 2;
	final int parallelism = numTaskManagers * numSlotsPerTaskManager;

	@Rule
	public final TemporaryFolder folder = new TemporaryFolder();

	@Test
	public void testTriggerSavepointAndResumeWithFileBasedCheckpoints() throws Exception {
		File testRoot = folder.getRoot();
		Configuration config = new Configuration();

		File checkpointDir = new File(testRoot, "checkpoints");
		File savepointRootDir = new File(testRoot, "savepoints");

		if (!checkpointDir.mkdir() || !savepointRootDir.mkdirs()) {
			fail("Test setup failed: failed to create temporary directories.");
		}

		String cpDir = checkpointDir.toURI().toString();
		MiniClusterResourceFactory clusterFactory = createCluster(numTaskManagers, numSlotsPerTaskManager, config,
				savepointRootDir, cpDir);

		String savepointPath = submitJobAndGetVerifiedSavepoint(clusterFactory, parallelism, cpDir);

		Savepoint savepoint = StateMetadataUtils.loadSavepoint(new Path(savepointPath));

		Path newSavepointPath = transformSavepoint(cpDir, savepoint);

		restoreJobAndVerifyState(savepointPath, clusterFactory, parallelism, newSavepointPath.toUri().toString());
	}

	private Path transformSavepoint(String cpDir, Savepoint savepoint) throws Exception, IOException {

		ExecutionEnvironment environment = ExecutionEnvironment.createLocalEnvironment();

		KeyedStateReader reader = new KeyedStateReader(savepoint, "hello", environment);

		DataSet<Tuple2<Integer, Integer>> countState = reader.readValueStates(
				ValueStateReader.forStateKVPairs("Count", new TypeHint<Tuple2<Integer, Integer>>() {}));

		DataSet<Tuple2<Integer, Integer>> newCountsToAdd = environment
				.fromElements(Tuple2.of(0, 100), Tuple2.of(3, 1000), Tuple2.of(1, 100), Tuple2.of(2, 1000));

		DataSet<Tuple2<Integer, Integer>> newStates = countState.join(newCountsToAdd).where(0).equalTo(0)
				.map(new SumValues());

		StateTransformer stateBuilder = new StateTransformer(savepoint, new Path(cpDir, "new"));
		DataSet<KeyedStateRow> newStateRows = stateBuilder.createKeyedStateRows("hello", "Count", newStates);

		stateBuilder.replaceKeyedState("hello", newStateRows.union(reader.getUnparsedStateRows()));
		stateBuilder.writeSavepointMetadata();

		return stateBuilder.getNewCheckpointPath();
	}

	private MiniClusterResourceFactory createCluster(final int numTaskManagers, final int numSlotsPerTaskManager,
			Configuration config, final File savepointRootDir, String cpDir) {
		config.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, cpDir);
		config.setInteger(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD, 0);
		config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointRootDir.toURI().toString());

		MiniClusterResourceFactory clusterFactory = new MiniClusterResourceFactory(numTaskManagers,
				numSlotsPerTaskManager, config);
		return clusterFactory;
	}

	private String submitJobAndGetVerifiedSavepoint(MiniClusterResourceFactory clusterFactory, int parallelism,
			String cpDir) throws Exception {
		final JobGraph jobGraph = createJobGraph(parallelism, cpDir);
		final JobID jobId = jobGraph.getJobID();

		MiniClusterResource cluster = clusterFactory.get();
		cluster.before();
		ClusterClient<?> client = cluster.getClusterClient();

		try {
			client.setDetached(true);
			client.submitJob(jobGraph, SavepointTransformationTest.class.getClassLoader());

			Thread.sleep(3000);

			String savepointPath = client.triggerSavepoint(jobId, null).get();

			return savepointPath;
		} finally {
			cluster.after();
		}
	}

	private void restoreJobAndVerifyState(String savepointPath, MiniClusterResourceFactory clusterFactory,
			int parallelism, String cpDir) throws Exception {

		final JobGraph jobGraph = createJobGraph(parallelism, cpDir);
		jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(cpDir));
		final JobID jobId = jobGraph.getJobID();
		MiniClusterResource cluster = clusterFactory.get();
		cluster.before();
		ClusterClient<?> client = cluster.getClusterClient();

		try {
			client.setDetached(true);
			client.submitJob(jobGraph, SavepointTransformationTest.class.getClassLoader());

			Thread.sleep(3000);

			client.cancel(jobId);

			FutureUtils.retrySuccesfulWithDelay(
					() -> client.getJobStatus(jobId),
					Time.milliseconds(50),
					Deadline.now().plus(Duration.ofSeconds(30)),
					status -> status == JobStatus.CANCELED,
					TestingUtils.defaultScheduledExecutor());
		} finally {
			cluster.after();
		}
	}

	private JobGraph createJobGraph(int parallelism, String cpDir) throws IOException {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);
		env.disableOperatorChaining();
		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(0, 1000));
		env.getConfig().disableSysoutLogging();
		env.setStateBackend(new RocksDBStateBackend(cpDir));

		env
				.addSource(new InfiniteTestSource())
				.keyBy(i -> i)
				.map(new Counter())
				.uid("hello")
				.addSink(new SinkFunction<Integer>() {});

		return env.getStreamGraph().getJobGraph();
	}

	// ------------------------------------------------------------------------
	// Utilities
	// ------------------------------------------------------------------------

	private static class MiniClusterResourceFactory {
		private final int numTaskManagers;
		private final int numSlotsPerTaskManager;
		private final Configuration config;

		private MiniClusterResourceFactory(int numTaskManagers, int numSlotsPerTaskManager, Configuration config) {
			this.numTaskManagers = numTaskManagers;
			this.numSlotsPerTaskManager = numSlotsPerTaskManager;
			this.config = config;
		}

		MiniClusterResource get() {
			return new MiniClusterResource(
					new MiniClusterResourceConfiguration.Builder()
							.setConfiguration(config)
							.setNumberTaskManagers(numTaskManagers)
							.setNumberSlotsPerTaskManager(numSlotsPerTaskManager)
							.build());
		}
	}
}
