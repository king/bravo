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

import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;

import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnappyStreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.king.bravo.api.KeyedStateRow;
import com.king.bravo.utils.SerializableSupplier;

public class KeyedStateWriterFunction
		extends RichGroupReduceFunction<KeyedStateRow, Tuple2<Integer, KeyedStateHandle>> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOGGER = LoggerFactory.getLogger(KeyedStateWriterFunction.class);

	private final int maxParallelism;
	private final int parallelism;
	private final Path opStateDir;
	private final SerializableSupplier<KeyedBackendSerializationProxy<?>> proxySupplier;

	private transient StreamCompressionDecorator keyGroupCompressionDecorator;
	private transient KeyedBackendSerializationProxy<?> serializationProxy;

	public KeyedStateWriterFunction(int maxParallelism, int parallelism,
			SerializableSupplier<KeyedBackendSerializationProxy<?>> proxySupplier,
			Path opStateDir) {
		this.maxParallelism = maxParallelism;
		this.parallelism = parallelism;
		this.proxySupplier = proxySupplier;
		this.opStateDir = opStateDir;
	}

	@Override
	public void open(Configuration c) throws IOException {
		serializationProxy = proxySupplier.get();
		keyGroupCompressionDecorator = serializationProxy.isUsingKeyGroupCompression()
				? SnappyStreamCompressionDecorator.INSTANCE
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
				previousStateId = nextRow.getStateId();

				opIndex = nextRow.getOperatorIndex(maxParallelism, parallelism);
				LOGGER.info("Writing to {}", checkpointFilePath);

				checkpointFileStream = checkpointFilePath.getFileSystem().create(checkpointFilePath,
						WriteMode.NO_OVERWRITE);

				// Write rocks keyed state metadata
				serializationProxy.write(new DataOutputViewStreamWrapper(checkpointFileStream));

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
				if (nextKeygroup != previousKeyGroup || nextRow.getStateId() != previousStateId) {
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
					kgOutView.writeShort(nextRow.getStateId());
				} else if (nextRow.getStateId() != previousStateId) {
					kgOutView.writeShort(nextRow.getStateId());
				}

				previousKeyGroup = nextKeygroup;
				previousStateId = nextRow.getStateId();
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