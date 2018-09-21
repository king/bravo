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

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.test.util.MiniClusterResource;
import org.apache.flink.util.TestLogger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.king.bravo.testing.actions.CancelJob;
import com.king.bravo.testing.actions.NextWatermark;
import com.king.bravo.testing.actions.Process;
import com.king.bravo.testing.actions.Sleep;
import com.king.bravo.testing.actions.TestPipelineSource;
import com.king.bravo.testing.actions.TriggerFailure;
import com.king.bravo.testing.actions.TriggerSavepoint;
import com.king.bravo.utils.StateMetadataUtils;

public abstract class BravoTestPipeline extends TestLogger implements Serializable {
	private static final long serialVersionUID = 1L;

	protected final Logger logger = LoggerFactory.getLogger(getClass());

	@Rule
	public final TemporaryFolder folder = new TemporaryFolder();

	public static JobGraph jobGraph;
	public static ClusterClient<?> client;
	public static JobID jobID;
	public static LinkedList<PipelineAction> actions = new LinkedList<>();

	@Before
	public void cleanOutputs() {
		CollectingSink.OUTPUT.clear();
		actions.clear();
	}

	public Path getLastCheckpointPath() throws IOException {
		FileStatus[] listStatus = FileSystem.getLocalFileSystem()
				.listStatus(new Path(getCheckpointDir(), jobID.toString()));

		return Arrays.stream(listStatus)
				.filter(s -> s.getPath().getName().startsWith("chk"))
				.sorted((s1, s2) -> -Integer.compare(
						Integer.parseInt(s1.getPath().getName().split("-")[1]),
						Integer.parseInt(s2.getPath().getName().split("-")[1])))
				.findFirst()
				.map(s -> s.getPath())
				.orElseThrow(() -> new IllegalStateException("Cannot find any checkpoints"));
	}

	public List<String> runTestPipeline(Function<DataStream<String>, DataStream<String>> pipelinerBuilder)
			throws Exception {
		return runTestPipeline(2, null, pipelinerBuilder);
	}

	public List<String> restoreTestPipelineFromSnapshot(String savepoint,
			Function<DataStream<String>, DataStream<String>> pipelinerBuilder) throws Exception {
		return runTestPipeline(2, savepoint, pipelinerBuilder);
	}

	public List<String> restoreTestPipelineFromLastCheckpoint(
			Function<DataStream<String>, DataStream<String>> pipelinerBuilder) throws Exception {
		return restoreTestPipelineFromSnapshot(getLastCheckpointPath().getPath(), pipelinerBuilder);
	}

	public List<String> restoreTestPipelineFromLastSavepoint(
			Function<DataStream<String>, DataStream<String>> pipelinerBuilder) throws Exception {
		if (TriggerSavepoint.lastSavepointPath == null) {
			throw new RuntimeException("triggerSavepoint must be called to obtain a valid savepoint");
		}
		return restoreTestPipelineFromSnapshot(TriggerSavepoint.lastSavepointPath, pipelinerBuilder);
	}

	private StreamExecutionEnvironment createJobGraph(int parallelism,
			Function<DataStream<String>, DataStream<String>> pipelinerBuilder) throws Exception {
		final Path checkpointDir = getCheckpointDir();
		final Path savepointRootDir = getSavepointDir();

		checkpointDir.getFileSystem().mkdirs(checkpointDir);
		savepointRootDir.getFileSystem().mkdirs(savepointRootDir);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableSysoutLogging();
		env.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setBufferTimeout(0);
		env.setParallelism(parallelism);
		env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);

		env.setStateBackend((StateBackend) new RocksDBStateBackend(checkpointDir.toString(), true));

		DataStream<String> sourceData = env
				.addSource(new TestPipelineSource())
				.uid("TestSource")
				.name("TestSource")
				.setParallelism(1);

		pipelinerBuilder.apply(sourceData)
				.addSink(new CollectingSink()).name("Output").uid("Output")
				.setParallelism(1);

		return env;
	}

	private List<String> runTestPipeline(int parallelism, String savepoint,
			Function<DataStream<String>, DataStream<String>> pipelinerBuilder) throws Exception {

		if (!actions.isEmpty() && actions.getLast() instanceof CancelJob
				&& ((CancelJob) actions.getLast()).isClusterActionTriggered()) {
			cancelJob();
		}

		jobGraph = createJobGraph(parallelism, pipelinerBuilder).getStreamGraph().getJobGraph();
		if (savepoint != null) {
			jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savepoint));
		}
		jobID = jobGraph.getJobID();

		MiniClusterResourceFactory clusterFactory = createCluster(1, 2);
		MiniClusterResource cluster = clusterFactory.get();
		cluster.before();
		client = cluster.getClusterClient();

		try {
			// client.setDetached(true);
			client.submitJob(jobGraph, BravoTestPipeline.class.getClassLoader());
		} catch (ProgramInvocationException pie) {
			if (!pie.getMessage().contains("Job was cancelled")) {
				throw pie;
			}
		} finally {
			cluster.after();
		}

		return CollectingSink.OUTPUT;
	}

	protected Path getCheckpointDir() {
		return new Path("file://" + folder.getRoot().getAbsolutePath(), "checkpoints");
	}

	protected Path getSavepointDir() {
		return new Path("file://" + folder.getRoot().getAbsolutePath(), "savepoints");
	}

	protected Path getLastSavepointPath() {
		return new Path(TriggerSavepoint.lastSavepointPath);
	}

	protected Savepoint getLastCheckpoint() throws IOException {
		return StateMetadataUtils.loadSavepoint(getLastCheckpointPath().getPath());
	}

	protected Savepoint getLastSavepoint() throws IOException {
		return StateMetadataUtils.loadSavepoint(getLastSavepointPath().getPath());
	}

	public void process(String element) {
		actions.add(new Process(element, 0));
	}

	public void process(String element, long ts) {
		actions.add(new Process(element, ts));
	}

	public void triggerFailure() {
		actions.add(new TriggerFailure());
	}

	public void triggerSavepoint() {
		actions.add(new TriggerSavepoint());
	}

	public void cancelJob() {
		actions.add(new CancelJob());
	}

	public void processWatermark(long timestamp) {
		actions.add(new NextWatermark(timestamp));
	}

	public void sleep(long millis) {
		actions.add(new Sleep(millis));
	}

	public void sleep(Time time) {
		sleep(time.toMilliseconds());
	}

	private MiniClusterResourceFactory createCluster(final int numTaskManagers,
			final int numSlotsPerTaskManager) {
		org.apache.flink.configuration.Configuration config = new org.apache.flink.configuration.Configuration();
		config.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, getCheckpointDir().toUri().toString());
		config.setInteger(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD, 0);
		config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, getSavepointDir().toUri().toString());

		MiniClusterResourceFactory clusterFactory = new MiniClusterResourceFactory(numTaskManagers,
				numSlotsPerTaskManager, config);
		return clusterFactory;
	}
}
