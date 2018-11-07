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

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;

@SuppressWarnings("serial")
public class KeyedStateInputSplit implements InputSplit {

	private final int splitNumber;
	private final OperatorSubtaskState subtaskState;

	public KeyedStateInputSplit(int splitNumber, OperatorSubtaskState subtaskState) {
		this.splitNumber = splitNumber;
		this.subtaskState = subtaskState;
	}

	@Override
	public int getSplitNumber() {
		return splitNumber;
	}

	public OperatorSubtaskState getOperatorSubtaskState() {
		return subtaskState;
	}

	@Override
	public String toString() {
		return new StringBuilder("OperatorStateInputSplit [")
				.append("splitNumber=").append(splitNumber)
				.append(", subtaskState=").append(subtaskState)
				.append("]")
				.toString();
	}

}
