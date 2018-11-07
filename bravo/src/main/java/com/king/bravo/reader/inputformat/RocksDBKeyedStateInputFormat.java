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
package com.king.bravo.reader.inputformat;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.stream.StreamSupport;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.util.IOUtils;

import com.king.bravo.types.KeyedStateRow;

/**
 * InputFormat for reading all {@link KeyedStateRow} for the specified state ids
 * (or all if empty) from an {@link OperatorState}.
 * 
 * Right now the input splits are created by subtaskstate, this could be
 * improved to be split by keygroup in the future
 */
public class RocksDBKeyedStateInputFormat extends RichInputFormat<KeyedStateRow, KeyedStateInputSplit> {

	private static final long serialVersionUID = 1L;
	private final OperatorState operatorState;

	private transient Iterator<KeyedStateRow> mergeIterator;

	private FilterFunction<String> stateFilter;
	private List<AutoCloseable> iterators = new ArrayList<>();

	public RocksDBKeyedStateInputFormat(OperatorState operatorState) {
		this(operatorState, i -> true);
	}

	public RocksDBKeyedStateInputFormat(OperatorState operatorState, FilterFunction<String> stateFilter) {
		this.operatorState = operatorState;
		this.stateFilter = stateFilter;
	}

	@Override
	public void open(KeyedStateInputSplit split) throws IOException {
		IOManagerAsync iomanager = new IOManagerAsync();
		String[] spillingDirectoriesPaths = iomanager.getSpillingDirectoriesPaths();

		mergeIterator = split.getOperatorSubtaskState()
				.getManagedKeyedState()
				.stream()
				.map(keyedStateHandle -> {
					if (keyedStateHandle instanceof IncrementalKeyedStateHandle) {
						File localDir = new File(spillingDirectoriesPaths[0], "rocksdb_" + UUID.randomUUID());
						if (!localDir.mkdirs()) {
							throw new RuntimeException("Could not create " + localDir);
						}
						RocksDBCheckpointIterator iterator = new RocksDBCheckpointIterator(
								(IncrementalKeyedStateHandle) keyedStateHandle,
								stateFilter, localDir.getAbsolutePath());
						iterators.add(iterator);
						return iterator;
					}

					if (!(keyedStateHandle instanceof KeyGroupsStateHandle)) {
						throw new IllegalStateException("Unexpected state handle type, " +
								"expected: " + KeyGroupsStateHandle.class +
								", but found: " + keyedStateHandle.getClass());
					} else {
						RocksDBSavepointIterator iterator = new RocksDBSavepointIterator(
								(KeyGroupsStateHandle) keyedStateHandle, stateFilter);
						iterators.add(iterator);
						return iterator;
					}
				}).flatMap(it -> StreamSupport.stream(it.spliterator(), false)).iterator();
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return !mergeIterator.hasNext();
	}

	@Override
	public KeyedStateRow nextRecord(KeyedStateRow reuse) throws IOException {
		return mergeIterator.next();
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
		return cachedStatistics;
	}

	@Override
	public KeyedStateInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		return operatorState.getSubtaskStates().entrySet().stream()
				.map(entry -> new KeyedStateInputSplit(entry.getKey(), entry.getValue()))
				.toArray(KeyedStateInputSplit[]::new);
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(KeyedStateInputSplit[] inputSplits) {
		return new DefaultInputSplitAssigner(inputSplits);
	}

	@Override
	public void close() throws IOException {
		IOUtils.closeAllQuietly(iterators);
		iterators.clear();
	}

	@Override
	public void configure(Configuration parameters) {}
}
