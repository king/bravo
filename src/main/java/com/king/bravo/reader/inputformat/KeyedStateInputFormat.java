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

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.runtime.checkpoint.OperatorState;

import com.king.bravo.api.KeyedStateRow;

/**
 * InputFormat for reading all {@link KeyedStateRow} for the specified state ids
 * (or all if empty) from an {@link OperatorState}.
 * 
 * Right now the input splits are created by subtaskstate, this could be
 * improved to be split by keygroup in the future
 */
public class KeyedStateInputFormat extends RichInputFormat<KeyedStateRow, KeyedStateInputSplit> {

	private static final long serialVersionUID = 1L;
	private final OperatorState operatorState;
	private final Set<Integer> targetStateIds;

	private transient CloseableRegistry closeableRegistry;
	private transient Iterator<KeyedStateRow> rowIt;

	private boolean reachedEnd;

	public KeyedStateInputFormat(OperatorState operatorState, Collection<Integer> targetStateIds) {
		this.operatorState = operatorState;
		this.targetStateIds = new HashSet<>(targetStateIds);
	}

	@Override
	public void openInputFormat() throws IOException {
		closeableRegistry = new CloseableRegistry();
	}

	@Override
	public void open(KeyedStateInputSplit split) throws IOException {
		rowIt = Stream.of(split.getOperatorSubtaskState())
				.flatMap(subtaskState -> subtaskState.getManagedKeyedState().stream())
				.map(keyedStateHandle -> new KeyedStateGroupReader(keyedStateHandle, targetStateIds))
				.peek(reader -> {
					try {
						closeableRegistry.registerCloseable(reader);
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				})
				.flatMap(KeyedStateGroupReader::stream)
				.iterator();
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return !rowIt.hasNext();
	}

	@Override
	public KeyedStateRow nextRecord(KeyedStateRow reuse) throws IOException {
		if (reachedEnd) {
			return null;
		}
		return rowIt.next();
	}

	@Override
	public void configure(Configuration parameters) {
		// nothing by default
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
		// nothing by default
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
		// nothing by default
	}

	@Override
	public void closeInputFormat() throws IOException {
		closeableRegistry.close();
	}

}
