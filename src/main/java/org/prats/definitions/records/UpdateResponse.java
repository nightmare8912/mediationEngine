package org.prats.definitions.records;

/** Typed wire payload for UpdateResponse. */
public record UpdateResponse(boolean ok, int affectedRows, String body) {}
