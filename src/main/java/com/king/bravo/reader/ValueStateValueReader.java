package com.king.bravo.reader;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;

import com.king.bravo.types.KeyedStateRow;

public class ValueStateValueReader<K, V> extends KeyedStateReader<K, V, V> {

	private static final long serialVersionUID = 1L;

	public ValueStateValueReader(String stateName, TypeInformation<V> outValueType) {
		super(stateName, null, outValueType, outValueType);
	}

	@Override
	public void flatMap(KeyedStateRow row, Collector<V> out) throws Exception {
		if (!stateName.equals(row.getStateName())) {
			return;
		}

		out.collect(InstantiationUtil.deserializeFromByteArray(valueDeserializer, row.getValueBytes()));
	}
}