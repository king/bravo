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
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnappyStreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.king.bravo.api.KeyedStateRow;

/**
 * Iterator over the raw keyed state stored in a RocksDB full snapshot
 * {@link KeyedStateHandle}. The logic is more or less extracted/replicated from
 * the RocksDBFullRestoreOperation inner class.
 * 
 * Optionally supports specifying a set of state ids to read, in this case
 * others are ignored.
 *
 */
public class KeyedStateGroupReader implements Iterator<KeyedStateRow>, Closeable, Iterable<KeyedStateRow> {

	private static final Logger LOGGER = LoggerFactory.getLogger(KeyedStateGroupReader.class);

	private final KeyGroupsStateHandle keyGroupsStateHandle;
	private final Set<Integer> targetStateIds;
	private final boolean filterStateIds;

	private FSDataInputStream stateHandleInStream;
	private Iterator<Long> offsetsIt;
	private DataInputViewStreamWrapper compressedInputView;

	private boolean hasNext = true;
	private int stateId;

	public KeyedStateGroupReader(KeyedStateHandle keyedStateHandle, Set<Integer> targetStateIds) {
		this.keyGroupsStateHandle = (KeyGroupsStateHandle) keyedStateHandle;
		this.targetStateIds = targetStateIds;
		filterStateIds = !targetStateIds.isEmpty();
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
		} catch (IOException e) {
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

	private final KeyedStateRow nextRecord(KeyedStateRow reuse) throws IOException {
		if (!openIfNeeded()) {
			return null;
		}

		byte[] key = BytePrimitiveArraySerializer.INSTANCE.deserialize(compressedInputView);
		byte[] value = BytePrimitiveArraySerializer.INSTANCE.deserialize(compressedInputView);

		reuse.f0 = (short) stateId;
		reuse.f1 = key;
		reuse.f2 = value;

		if (hasMetaDataFollowsFlag(reuse.f1)) {
			clearMetaDataFollowsFlag(reuse.f1);

			stateId = seekNextStateId(true);
			while (stateId == END_OF_KEY_GROUP_MARK && hasNext) {
				hasNext = seekNextOffset();
				if (hasNext) {
					stateId = seekNextStateId(false);
				}
			}
		}

		LOGGER.trace("{}", reuse);

		return reuse;
	}

	private boolean openIfNeeded() throws IOException {
		if (stateHandleInStream == null) {
			LOGGER.debug("Opening {}", keyGroupsStateHandle.getDelegateStateHandle());
			stateHandleInStream = keyGroupsStateHandle.openInputStream();

			final DataInputViewStreamWrapper stateHandleInView = new DataInputViewStreamWrapper(stateHandleInStream);

			final KeyedBackendSerializationProxy<?> serializationProxy = new KeyedBackendSerializationProxy<>(
					getClass().getClassLoader(), false);
			serializationProxy.read(stateHandleInView);

			final StreamCompressionDecorator streamCompressionDecorator = serializationProxy
					.isUsingKeyGroupCompression()
							? SnappyStreamCompressionDecorator.INSTANCE
							: UncompressedStreamCompressionDecorator.INSTANCE;

			final KeyGroupRangeOffsets rangeOffsets = keyGroupsStateHandle.getGroupRangeOffsets();
			LOGGER.debug("{}", rangeOffsets);

			offsetsIt = new ValidOffsetsIterator(rangeOffsets);

			hasNext = seekNextOffset();

			if (hasNext) {
				final InputStream compressedInputStream = streamCompressionDecorator
						.decorateWithCompression(stateHandleInStream);
				compressedInputView = new DataInputViewStreamWrapper(compressedInputStream);
				stateId = seekNextStateId(false);
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

	private int seekNextStateId(boolean metaFollows) throws IOException {

		int stateId = compressedInputView.readShort();
		if (metaFollows) {
			stateId = END_OF_KEY_GROUP_MARK & stateId;
		}

		while (filterStateIds && !targetStateIds.contains(stateId) && stateId != END_OF_KEY_GROUP_MARK) {

			final int keySize = compressedInputView.readInt();
			final byte keyByte0 = compressedInputView.readByte();
			compressedInputView.skip(keySize - 1);

			final int valueSize = compressedInputView.readInt();
			compressedInputView.skip(valueSize);

			if (hasMetaDataFollowsFlag(keyByte0)) {
				stateId = END_OF_KEY_GROUP_MARK & compressedInputView.readShort();
			}
		}

		return stateId;
	}

	public Stream<KeyedStateRow> stream() {
		return StreamSupport.stream(spliterator(), false);
	}

	@Override
	public Iterator<KeyedStateRow> iterator() {
		return this;
	}
}
