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

import com.king.bravo.types.KeyedStateRow;
import com.king.bravo.utils.StateMetadataUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.RocksIteratorWrapper;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class RocksDBCheckpointIterator implements Iterator<KeyedStateRow>, Closeable, Iterable<KeyedStateRow> {

	private final DBOptions dbOptions = new DBOptions();
	private final ColumnFamilyOptions colOptions = new ColumnFamilyOptions()
			.setMergeOperatorName(RocksDBKeyedStateBackend.MERGE_OPERATOR_NAME);

	private final RocksDB db;
	private LinkedList<Entry<String, RocksIteratorWrapper>> iteratorQueue;

	private String currentName;
	private RocksIteratorWrapper currentIterator;

	private ArrayList<ColumnFamilyHandle> stateColumnFamilyHandles;
	private CloseableRegistry cancelStreamRegistry;
	private String localPath;

	public RocksDBCheckpointIterator(IncrementalRemoteKeyedStateHandle handle, FilterFunction<String> stateFilter,
									 String localPath) {
		this.localPath = localPath;
		this.cancelStreamRegistry = new CloseableRegistry();
		List<StateMetaInfoSnapshot> stateMetaInfoSnapshots = StateMetadataUtils
				.getKeyedBackendSerializationProxy(handle.getMetaStateHandle()).getStateMetaInfoSnapshots();

		stateColumnFamilyHandles = new ArrayList<>(stateMetaInfoSnapshots.size() + 1);
		List<ColumnFamilyDescriptor> stateColumnFamilyDescriptors = createAndRegisterColumnFamilyDescriptors(
				stateMetaInfoSnapshots);
		try {
			transferAllStateDataToDirectory(handle, new Path(localPath));
			this.db = openDB(localPath, stateColumnFamilyDescriptors, stateColumnFamilyHandles);
			createColumnIterators(stateFilter, stateMetaInfoSnapshots);
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	private void transferAllStateDataToDirectory(
			IncrementalRemoteKeyedStateHandle restoreStateHandle,
			Path dest) throws IOException {

		final Map<StateHandleID, StreamStateHandle> sstFiles = restoreStateHandle.getSharedState();
		final Map<StateHandleID, StreamStateHandle> miscFiles = restoreStateHandle.getPrivateState();

		transferAllDataFromStateHandles(sstFiles, dest);
		transferAllDataFromStateHandles(miscFiles, dest);
	}

	private void transferAllDataFromStateHandles(
			Map<StateHandleID, StreamStateHandle> stateHandleMap,
			Path restoreInstancePath) throws IOException {

		for (Map.Entry<StateHandleID, StreamStateHandle> entry : stateHandleMap.entrySet()) {
			StateHandleID stateHandleID = entry.getKey();
			StreamStateHandle remoteFileHandle = entry.getValue();
			copyStateDataHandleData(new Path(restoreInstancePath, stateHandleID.toString()), remoteFileHandle);
		}
	}

	private void copyStateDataHandleData(
			Path restoreFilePath,
			StreamStateHandle remoteFileHandle) throws IOException {

		FileSystem restoreFileSystem = restoreFilePath.getFileSystem();

		FSDataInputStream inputStream = null;
		FSDataOutputStream outputStream = null;

		try {
			inputStream = remoteFileHandle.openInputStream();
			cancelStreamRegistry.registerCloseable(inputStream);

			outputStream = restoreFileSystem.create(restoreFilePath, FileSystem.WriteMode.OVERWRITE);
			cancelStreamRegistry.registerCloseable(outputStream);

			byte[] buffer = new byte[8 * 1024];
			while (true) {
				int numBytes = inputStream.read(buffer);
				if (numBytes == -1) {
					break;
				}

				outputStream.write(buffer, 0, numBytes);
			}
		} finally {
			if (cancelStreamRegistry.unregisterCloseable(inputStream)) {
				inputStream.close();
			}

			if (cancelStreamRegistry.unregisterCloseable(outputStream)) {
				outputStream.close();
			}
		}
	}

	private void createColumnIterators(FilterFunction<String> stateFilter,
			List<StateMetaInfoSnapshot> stateMetaInfoSnapshots)
			throws Exception {
		Map<String, RocksIteratorWrapper> iterators = new HashMap<>();
		for (int i = 0; i < stateMetaInfoSnapshots.size(); i++) {
			String name = stateMetaInfoSnapshots.get(i).getName();
			if (stateFilter.filter(name)) {
				RocksIteratorWrapper iterator = new RocksIteratorWrapper(
						this.db.newIterator(stateColumnFamilyHandles.get(i + 1)));
				iterators.put(name, iterator);
				iterator.seekToFirst();
			}
		}

		iteratorQueue = new LinkedList<>(iterators.entrySet());
		updateCurrentIterator();
	}

	private void updateCurrentIterator() {
		IOUtils.closeQuietly(currentIterator);
		if (iteratorQueue.isEmpty()) {
			currentIterator = null;
			currentName = null;
			return;
		} else {
			Entry<String, RocksIteratorWrapper> e = iteratorQueue.pop();
			currentName = e.getKey();
			currentIterator = e.getValue();
		}
	}

	private List<ColumnFamilyDescriptor> createAndRegisterColumnFamilyDescriptors(
			List<StateMetaInfoSnapshot> stateMetaInfoSnapshots) {

		List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>(stateMetaInfoSnapshots.size());

		for (StateMetaInfoSnapshot stateMetaInfoSnapshot : stateMetaInfoSnapshots) {
			ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(
					stateMetaInfoSnapshot.getName().getBytes(ConfigConstants.DEFAULT_CHARSET),
					colOptions);

			columnFamilyDescriptors.add(columnFamilyDescriptor);
		}
		return columnFamilyDescriptors;
	}

	private RocksDB openDB(String path, List<ColumnFamilyDescriptor> stateColumnFamilyDescriptors,
			List<ColumnFamilyHandle> stateColumnFamilyHandles) throws IOException {

		List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>(1 + stateColumnFamilyDescriptors.size());

		// we add the required descriptor for the default CF in FIRST position,
		// see
		// https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#opening-a-database-with-column-families
		columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, colOptions));
		columnFamilyDescriptors.addAll(stateColumnFamilyDescriptors);

		try {
			return RocksDB.open(
					dbOptions,
					Preconditions.checkNotNull(path),
					columnFamilyDescriptors,
					stateColumnFamilyHandles);
		} catch (RocksDBException e) {
			throw new IOException("Error while opening RocksDB instance.", e);
		}
	}

	@Override
	public final boolean hasNext() {
		if (currentIterator == null) {
			return false;
		}

		if (currentIterator.isValid()) {
			return true;
		} else {
			updateCurrentIterator();
			return hasNext();
		}
	}

	@Override
	public final KeyedStateRow next() {
		byte[] key = currentIterator.key();
		byte[] value = currentIterator.value();
		currentIterator.next();
		return new KeyedStateRow(currentName, key, value);
	}

	@Override
	public void close() throws IOException {
		IOUtils.closeQuietly(cancelStreamRegistry);
		IOUtils.closeAllQuietly(stateColumnFamilyHandles);
		IOUtils.closeQuietly(db);
		IOUtils.closeQuietly(dbOptions);
		IOUtils.closeQuietly(colOptions);
		FileUtils.deleteDirectoryQuietly(new File(localPath));
	}

	@Override
	public Iterator<KeyedStateRow> iterator() {
		return this;
	}
}
