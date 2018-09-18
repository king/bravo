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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.contrib.streaming.state.RocksIteratorWrapper;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import com.king.bravo.types.KeyedStateRow;
import com.king.bravo.utils.StateMetadataUtils;

public class RocksDBCheckpointIterator implements Iterator<KeyedStateRow>, Closeable, Iterable<KeyedStateRow> {

	private final DBOptions dbOptions = new DBOptions();
	private final ColumnFamilyOptions colOptions = new ColumnFamilyOptions();
	private final RocksDB db;
	private final LinkedList<Entry<String, RocksIteratorWrapper>> iteratorQueue;

	private String currentName;
	private RocksIteratorWrapper currentIterator;

	private ArrayList<ColumnFamilyHandle> stateColumnFamilyHandles;

	public RocksDBCheckpointIterator(IncrementalKeyedStateHandle handle, FilterFunction<String> stateFilter,
			String rocksdbLocalPaths) throws Exception {
		List<StateMetaInfoSnapshot> stateMetaInfoSnapshots = StateMetadataUtils
				.getKeyedBackendSerializationProxy(handle.getMetaStateHandle()).getStateMetaInfoSnapshots();

		stateColumnFamilyHandles = new ArrayList<>(stateMetaInfoSnapshots.size() + 1);
		List<ColumnFamilyDescriptor> stateColumnFamilyDescriptors = createAndRegisterColumnFamilyDescriptors(
				stateMetaInfoSnapshots);
		this.db = openDB(rocksdbLocalPaths, stateColumnFamilyDescriptors, stateColumnFamilyHandles);

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

	private RocksDB openDB(
			String path,
			List<ColumnFamilyDescriptor> stateColumnFamilyDescriptors,
			List<ColumnFamilyHandle> stateColumnFamilyHandles) throws IOException {

		List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>(1 + stateColumnFamilyDescriptors.size());

		// we add the required descriptor for the default CF in FIRST position, see
		// https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#opening-a-database-with-column-families
		columnFamilyDescriptors
				.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, colOptions));
		columnFamilyDescriptors.addAll(stateColumnFamilyDescriptors);

		RocksDB dbRef;

		try {
			dbRef = RocksDB.open(
					dbOptions,
					Preconditions.checkNotNull(path),
					columnFamilyDescriptors,
					stateColumnFamilyHandles);
		} catch (RocksDBException e) {
			throw new IOException("Error while opening RocksDB instance.", e);
		}

		// requested + default CF
		Preconditions.checkState(1 + stateColumnFamilyDescriptors.size() == stateColumnFamilyHandles.size(),
				"Not all requested column family handles have been created");

		return dbRef;
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
		for (ColumnFamilyHandle columnFamilyHandle : stateColumnFamilyHandles) {
			IOUtils.closeQuietly(columnFamilyHandle);
		}

		IOUtils.closeQuietly(db);
	}

	public Stream<KeyedStateRow> stream() {
		return StreamSupport.stream(spliterator(), false);
	}

	@Override
	public Iterator<KeyedStateRow> iterator() {
		return this;
	}
}
