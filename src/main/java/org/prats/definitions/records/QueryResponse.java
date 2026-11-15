package org.prats.definitions.records;

/** Typed wire payload for QueryResponse. */
public record QueryResponse(boolean ok, java.util.List<String> columns, java.util.List<java.util.Map<String, Object>> rows, int rowCount, boolean truncated, String body) {}
