package com.example.batchjpa.model;

import java.util.Collections;
import java.util.Map;

/**
 * Immutable DTO that carries one parsed line from the flat file.
 *
 * <p>Using a Java {@code record} keeps the model concise and thread-safe.
 * The {@link com.example.batchjpa.reader.DynamicLineMapper} constructs these;
 * the {@link com.example.batchjpa.processor.DynamicDataRecordProcessor}
 * converts them to {@link com.example.batchjpa.entity.DataRecord} entities.
 *
 * @param recordKey  Value of the first column (used as the entity primary key).
 * @param columns    All column-name → column-value pairs for the row,
 *                   including the first column entry so processors have the
 *                   full picture if needed.  The map is unmodifiable.
 */
public record DynamicDataRecord(
        String recordKey,
        Map<String, String> columns
) {

    /**
     * Compact canonical constructor: defensively copies the map and makes it
     * unmodifiable so that no caller can mutate the record after creation.
     */
    public DynamicDataRecord {
        if (recordKey == null || recordKey.isBlank()) {
            throw new IllegalArgumentException("recordKey must not be null or blank");
        }
        columns = Collections.unmodifiableMap(Map.copyOf(columns));
    }
}
