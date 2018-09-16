package com.king.bravo.writer.functions;

import java.util.HashSet;
import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;

import com.king.bravo.types.KeyedStateRow;

public final class RowFilter implements FilterFunction<KeyedStateRow> {
	private static final long serialVersionUID = 1L;
	private final Set<String> states;

	public RowFilter(Set<String> states) {
		this.states = new HashSet<>(states);
	}

	@Override
	public boolean filter(KeyedStateRow row) throws Exception {
		return states.contains(row.getStateName());
	}
}