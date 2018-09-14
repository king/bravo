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

import static com.king.bravo.utils.KeyGroupFlags.END_OF_KEY_GROUP_MARK;
import static com.king.bravo.utils.KeyGroupFlags.clearMetaDataFollowsFlag;
import static com.king.bravo.utils.KeyGroupFlags.hasMetaDataFollowsFlag;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.king.bravo.types.KeyedStateRow;
import com.king.bravo.utils.StateMetadataUtils;

/**
 * Iterator over the raw keyed state stored in a RocksDB full snapshot
 * {@link KeyedStateHandle}. The logic is more or less extracted/replicated from
 * the RocksDBFullRestoreOperation inner class.
 * 
 * Optionally supports specifying a set of state ids to read, in this case
 * others are ignored.
 *
 */
public class RocksDBSavepointIterator implements Iterator<KeyedStateRow>, Closeable, Iterable<KeyedStateRow> {

	private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBSavepointIterator.class);

	private final KeyGroupsStateHandle keyGroupsStateHandle;

	private FSDataInputStream stateHandleInStream;
	private Iterator<Long> offsetsIt;
	private DataInputViewStreamWrapper compressedInputView;
	private Map<Integer, String> stateIdMapping;

	private boolean hasNext = true;
	private int stateId;

	private String stateName;

	private final FilterFunction<String> stateFilter;

	public RocksDBSavepointIterator(KeyGroupsStateHandle keyedStateHandle, FilterFunction<String> stateFilter) {
		this.stateFilter = stateFilter;
		this.keyGroupsStateHandle = keyedStateHandle;
	}

	@Override
	public final boolean hasNext() {
		return hasNext;
	}

	@Override
	public final KeyedStateRow next() {
		try {
			// TODO reduce GC pressure
			// return nextRecord(reuse);
			return nextRecord(new KeyedStateRow());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() throws IOException {
		if (stateHandleInStream != null) {
			LOGGER.debug("Closing {}", this.keyGroupsStateHandle.getDelegateStateHandle());
			stateHandleInStream.close();
		}
	}

	private final KeyedStateRow nextRecord(KeyedStateRow reuse) throws Exception {
		if (!openIfNeeded()) {
			return null;
		}

		byte[] key = BytePrimitiveArraySerializer.INSTANCE.deserialize(compressedInputView);
		byte[] value = BytePrimitiveArraySerializer.INSTANCE.deserialize(compressedInputView);

		reuse.f0 = stateName;
		reuse.f1 = key;
		reuse.f2 = value;

		if (hasMetaDataFollowsFlag(reuse.f1)) {
			clearMetaDataFollowsFlag(reuse.f1);

			seekNextStateId(true);
			while (stateId == END_OF_KEY_GROUP_MARK && hasNext) {
				hasNext = seekNextOffset();
				if (hasNext) {
					seekNextStateId(false);
				}
			}
		}

		LOGGER.trace("{}", reuse);

		return reuse;
	}

	private boolean openIfNeeded() throws Exception {
		if (stateHandleInStream == null) {
			LOGGER.debug("Opening {}", keyGroupsStateHandle.getDelegateStateHandle());
			stateHandleInStream = keyGroupsStateHandle.openInputStream();

			final KeyedBackendSerializationProxy<?> serializationProxy = StateMetadataUtils
					.getKeyedBackendSerializationProxy(keyGroupsStateHandle);

			this.stateIdMapping = StateMetadataUtils.getStateIdMapping(serializationProxy);
			final StreamCompressionDecorator streamCompressionDecorator = StateMetadataUtils
					.getCompressionDecorator(serializationProxy);

			final KeyGroupRangeOffsets rangeOffsets = keyGroupsStateHandle.getGroupRangeOffsets();
			LOGGER.debug("{}", rangeOffsets);

			offsetsIt = new ValidOffsetsIterator(rangeOffsets);

			hasNext = seekNextOffset();

			if (hasNext) {
				final InputStream compressedInputStream = streamCompressionDecorator
						.decorateWithCompression(stateHandleInStream);
				compressedInputView = new DataInputViewStreamWrapper(compressedInputStream);
				seekNextStateId(false);
			}
		}

		return hasNext;
	}

	private boolean seekNextOffset() throws IOException {
		final boolean hasNext = offsetsIt.hasNext();
		if (hasNext) {
			final long offset = offsetsIt.next();
			LOGGER.debug("Seeking offset {}", offset);
			stateHandleInStream.seek(offset);
		}
		return hasNext;
	}

	private void seekNextStateId(boolean metaFollows) throws Exception {

		int stateId = compressedInputView.readShort();
		String stateName = stateIdMapping.get(stateId);
		if (metaFollows) {
			stateId = END_OF_KEY_GROUP_MARK & stateId;
		}

		while (!stateFilter.filter(stateName) && stateId != END_OF_KEY_GROUP_MARK) {

			final int keySize = compressedInputView.readInt();
			final byte keyByte0 = compressedInputView.readByte();
			compressedInputView.skip(keySize - 1);

			final int valueSize = compressedInputView.readInt();
			compressedInputView.skip(valueSize);

			if (hasMetaDataFollowsFlag(keyByte0)) {
				stateId = END_OF_KEY_GROUP_MARK & compressedInputView.readShort();
				stateName = stateIdMapping.get(stateId);
			}
		}

		this.stateId = stateId;
		this.stateName = stateName;
	}

	public Stream<KeyedStateRow> stream() {
		return StreamSupport.stream(spliterator(), false);
	}

	@Override
	public Iterator<KeyedStateRow> iterator() {
		return this;
	}
}
