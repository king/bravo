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
package com.king.bravo.writer.functions;

import static com.king.bravo.utils.KeyGroupFlags.END_OF_KEY_GROUP_MARK;
import static com.king.bravo.utils.KeyGroupFlags.setMetaDataFollowsFlagInKey;

import java.io.OutputStream;
import java.util.Map;
import java.util.UUID;

import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnappyStreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.king.bravo.types.KeyedStateRow;

public class RocksDBSavepointWriter extends RichGroupReduceFunction<KeyedStateRow, Tuple2<Integer, KeyedStateHandle>> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBSavepointWriter.class);

	private StreamCompressionDecorator keyGroupCompressionDecorator;
	private Map<String, Integer> stateIdMapping;

	private final int maxParallelism;
	private final int parallelism;
	private final Path opStateDir;
	private byte[] metaBytes;

	public RocksDBSavepointWriter(int maxParallelism, int parallelism, Map<String, Integer> stateIdMapping,
			boolean compression, Path opStateDir, byte[] metaBytes) {

		this.maxParallelism = maxParallelism;
		this.parallelism = parallelism;
		this.stateIdMapping = stateIdMapping;
		this.opStateDir = opStateDir;
		this.metaBytes = metaBytes;
		this.keyGroupCompressionDecorator = compression ? SnappyStreamCompressionDecorator.INSTANCE
				: UncompressedStreamCompressionDecorator.INSTANCE;
	}

	@Override
	public void reduce(Iterable<KeyedStateRow> values,
			Collector<Tuple2<Integer, KeyedStateHandle>> out)
			throws Exception {

		Path checkpointFilePath = new Path(opStateDir, String.valueOf(UUID.randomUUID()));
		FSDataOutputStream checkpointFileStream = null;

		KeyGroupRangeOffsets keyGroupRangeOffsets = null;
		int opIndex = -1;

		DataOutputView kgOutView = null;
		OutputStream kgOutStream = null;

		try {

			byte[] previousKey = null;
			byte[] previousValue = null;
			int previousKeyGroup = -1;
			int previousStateId = -1;

			java.util.Iterator<KeyedStateRow> iterator = values.iterator();

			KeyedStateRow nextRow;
			if (iterator.hasNext()) {
				nextRow = iterator.next();
				previousKeyGroup = nextRow.getKeyGroup(maxParallelism);
				opIndex = nextRow.getOperatorIndex(maxParallelism, parallelism);
				previousStateId = stateIdMapping.get(nextRow.getStateName());

				LOGGER.info("Writing to {}", checkpointFilePath);

				checkpointFileStream = checkpointFilePath.getFileSystem().create(checkpointFilePath,
						WriteMode.NO_OVERWRITE);

				// Write rocks keyed state metadata
				checkpointFileStream.write(metaBytes);

				keyGroupRangeOffsets = new KeyGroupRangeOffsets(
						KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(maxParallelism,
								parallelism, opIndex));

				// begin first key-group by recording the offset
				keyGroupRangeOffsets.setKeyGroupOffset(
						previousKeyGroup,
						checkpointFileStream.getPos());

				kgOutStream = keyGroupCompressionDecorator.decorateWithCompression(checkpointFileStream);
				kgOutView = new DataOutputViewStreamWrapper(kgOutStream);
				kgOutView.writeShort(previousStateId);

				previousKey = nextRow.getKeyAndNamespaceBytes();
				previousValue = nextRow.getValueBytes();
			}

			// main loop: write k/v pairs ordered by (key-group, kv-state), thereby tracking
			// key-group offsets.
			while (iterator.hasNext()) {
				nextRow = iterator.next();

				// set signal in first key byte that meta data will follow in the stream after
				// this k/v pair
				int nextKeygroup = nextRow.getKeyGroup(maxParallelism);
				if (nextKeygroup != previousKeyGroup || stateIdMapping.get(nextRow.getStateName()) != previousStateId) {
					setMetaDataFollowsFlagInKey(previousKey);
				}

				BytePrimitiveArraySerializer.INSTANCE.serialize(previousKey, kgOutView);
				BytePrimitiveArraySerializer.INSTANCE.serialize(previousValue, kgOutView);

				// write meta data if we have to
				if (nextKeygroup != previousKeyGroup) {
					kgOutView.writeShort(END_OF_KEY_GROUP_MARK);
					// this will just close the outer stream
					kgOutStream.close();
					// begin new key-group
					keyGroupRangeOffsets.setKeyGroupOffset(
							nextRow.getKeyGroup(maxParallelism),
							checkpointFileStream.getPos());

					kgOutStream = keyGroupCompressionDecorator.decorateWithCompression(checkpointFileStream);
					kgOutView = new DataOutputViewStreamWrapper(kgOutStream);
					kgOutView.writeShort(stateIdMapping.get(nextRow.getStateName()));
				} else if (stateIdMapping.get(nextRow.getStateName()) != previousStateId) {
					kgOutView.writeShort(stateIdMapping.get(nextRow.getStateName()));
				}

				previousKeyGroup = nextKeygroup;
				previousStateId = stateIdMapping.get(nextRow.getStateName());
				previousKey = nextRow.getKeyAndNamespaceBytes();
				previousValue = nextRow.getValueBytes();
			}

			if (previousKey != null) {
				setMetaDataFollowsFlagInKey(previousKey);
				BytePrimitiveArraySerializer.INSTANCE.serialize(previousKey, kgOutView);
				BytePrimitiveArraySerializer.INSTANCE.serialize(previousValue, kgOutView);
				kgOutView.writeShort(END_OF_KEY_GROUP_MARK);
				kgOutStream.close();
				kgOutStream = null;
			}
		} finally {
			if (checkpointFileStream != null) {
				checkpointFileStream.close();
			}
		}
		out.collect(Tuple2.of(opIndex,
				new KeyGroupsStateHandle(keyGroupRangeOffsets, new FileStateHandle(checkpointFilePath, 0))));
	}
}