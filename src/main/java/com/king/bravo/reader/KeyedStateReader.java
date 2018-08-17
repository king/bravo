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
package com.king.bravo.reader;

import java.util.Collection;
import java.util.Collections;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;

import com.king.bravo.api.KeyedStateRow;
import com.king.bravo.reader.inputformat.KeyedStateInputFormat;
import com.king.bravo.utils.StateMetadataUtils;
import com.king.bravo.writer.StateTransformer;

/**
 * Convenience object for reading keyed states from a given {@link Savepoint}
 * and operator uid. <br>
 * <br>
 * Typical flow:
 * <ol>
 * <li>Create a KeyedStateReader object for the operator</li>
 * <li>Use readValueStates to read some value states into DataSets</li>
 * <li>Transform these DataSets</li>
 * <li>Use {@link StateTransformer} to create a new state</li>
 * </ol>
 */
public class KeyedStateReader {

	private final DataSet<KeyedStateRow> allRows;
	private final OperatorState opState;

	private DataSet<KeyedStateRow> unparsedRows;

	private KeyedStateReader(OperatorState opState, DataSet<KeyedStateRow> unparsedState) {
		this.opState = opState;
		this.allRows = unparsedState;
		this.unparsedRows = allRows;
	}

	private KeyedStateReader(OperatorState opState, ExecutionEnvironment env, Collection<String> stateNames) {
		this(opState, env.createInput(
				new KeyedStateInputFormat(opState, StateMetadataUtils.getKeyedStateIds(opState, stateNames).values())));
	}

	public KeyedStateReader(Savepoint sp, String uid, ExecutionEnvironment env) {
		this(StateMetadataUtils.getOperatorState(sp, uid), env, Collections.emptyList());
	}

	public KeyedStateReader(Savepoint sp, String uid, ExecutionEnvironment env, Collection<String> stateNames) {
		this(StateMetadataUtils.getOperatorState(sp, uid), env, stateNames);
	}

	/**
	 * Read the value states using the provided reader for further processing
	 * 
	 * @return The DataSet containing the deseralized state keys and values
	 *         depending on the reader
	 */
	public <K, V, O> DataSet<O> readValueStates(ValueStateReader<K, V, O> parser) throws Exception {
		parser.init(opState);
		DataSet<O> parsedState = allRows.flatMap(parser);
		unparsedRows = unparsedRows.filter(ksr -> parser.getParserStateId() != ksr.getStateId());

		return parsedState;
	}

	/**
	 * @return DataSet containing all keyed states of the operator
	 */
	public DataSet<KeyedStateRow> getAllStateRows() {
		return allRows;
	}

	/**
	 * Returm ll the keyed state rows that were not accessed using a reader. This is
	 * a convenience method so we can union the untouched part of the state with the
	 * changed parts before writing them back.
	 */
	public DataSet<KeyedStateRow> getUnparsedStateRows() {
		return unparsedRows;
	}
}