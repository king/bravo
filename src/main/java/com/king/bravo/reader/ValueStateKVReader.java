package com.king.bravo.reader;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;

import com.king.bravo.types.KeyedStateRow;
import com.king.bravo.utils.RocksDBKeySerializationUtils;

public class ValueStateKVReader<K, V> extends KeyedStateReader<K, V, Tuple2<K, V>> {

	private static final long serialVersionUID = 1L;

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public ValueStateKVReader(String stateName, TypeInformation<K> outKeyType, TypeInformation<V> outValueType) {
		super(stateName, outKeyType, outValueType, new TupleTypeInfo(Tuple2.class, outKeyType, outValueType));
	}

	@Override
	public void flatMap(KeyedStateRow row, Collector<Tuple2<K, V>> out) throws Exception {
		if (!stateName.equals(row.getStateName())) {
			return;
		}

		byte[] keyAndNamespaceBytes = row.getKeyAndNamespaceBytes();
		byte[] valueBytes = row.getValueBytes();

		K key;
		try (ByteArrayInputStreamWithPos keyIs = new ByteArrayInputStreamWithPos(keyAndNamespaceBytes)) {
			DataInputViewStreamWrapper iw = new DataInputViewStreamWrapper(keyIs);
			iw.skipBytesToRead(keygroupPrefixBytes);
			key = RocksDBKeySerializationUtils.readKey(keyDeserializer, keyIs, iw, false);
		}

		V value = InstantiationUtil.deserializeFromByteArray(valueDeserializer, valueBytes);
		out.collect(Tuple2.of(key, value));
	}
}