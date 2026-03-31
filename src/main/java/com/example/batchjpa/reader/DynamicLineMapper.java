package com.example.batchjpa.reader;

import com.example.batchjpa.model.DynamicDataRecord;
import org.springframework.batch.item.file.LineMapper;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Stateless {@link LineMapper} that maps a delimited text line to a
 * {@link DynamicDataRecord} using the column names discovered at runtime from
 * the file header.
 *
 * <p>This class is a plain POJO – not a Spring component – because it is
 * constructed programmatically inside the {@code @StepScope} reader bean in
 * {@link com.example.batchjpa.config.BatchConfig}, where the column names and
 * delimiter are already resolved from the {@code JobExecutionContext}.
 *
 * <h3>Design notes</h3>
 * <ul>
 *   <li>The first column value becomes {@code recordKey} (the entity PK).</li>
 *   <li>All columns (including the first) are stored in the {@code columns}
 *       map so downstream processors have the full row available.</li>
 *   <li>If a data row has <em>fewer</em> tokens than headers the missing
 *       values are stored as empty strings, preventing index-out-of-bounds
 *       errors on ragged lines.</li>
 *   <li>Extra tokens beyond the header count are silently ignored.</li>
 * </ul>
 *
 * @param columnNames ordered array of column names taken from the file header
 * @param delimiter   field separator character(s), e.g. {@code "|"}
 */
public record DynamicLineMapper(
        String[] columnNames,
        String delimiter
) implements LineMapper<DynamicDataRecord> {

    /**
     * Compact canonical constructor: validates inputs eagerly so failures are
     * reported at construction time rather than during chunk processing.
     */
    public DynamicLineMapper {
        if (columnNames == null || columnNames.length == 0) {
            throw new IllegalArgumentException("columnNames must not be null or empty");
        }
        if (delimiter == null || delimiter.isEmpty()) {
            throw new IllegalArgumentException("delimiter must not be null or empty");
        }
        // Defensive copy so external mutations do not affect this mapper
        columnNames = columnNames.clone();
    }

    /**
     * Parses {@code line} into a {@link DynamicDataRecord}.
     *
     * <p>{@code lineNumber} is provided by Spring Batch but is not used here
     * because the mapping is purely positional based on the header.
     *
     * @param line       raw text line from the flat file (never {@code null})
     * @param lineNumber 1-based line number within the file (unused)
     * @return a fully populated {@link DynamicDataRecord}
     * @throws IllegalArgumentException if the line is blank (should not reach
     *         here in normal operation because the reader skips the header and
     *         stops before the footer)
     */
    @Override
    public DynamicDataRecord mapLine(String line, int lineNumber) {

        if (line == null || line.isBlank()) {
            throw new IllegalArgumentException(
                    "Blank line encountered at line %d – check maxItemCount / linesToSkip settings"
                            .formatted(lineNumber));
        }

        // Split on the literal delimiter – pipe (|) must not be treated as a
        // regex alternation operator, so we quote it.
        String[] tokens = line.split(java.util.regex.Pattern.quote(delimiter), -1);

        Map<String, String> columns = LinkedHashMap.newLinkedHashMap(columnNames.length);

        for (int i = 0; i < columnNames.length; i++) {
            String value = (i < tokens.length) ? tokens[i].trim() : "";
            columns.put(columnNames[i], value);
        }

        // First column value is the record key
        String recordKey = columns.get(columnNames[0]);

        return new DynamicDataRecord(recordKey, columns);
    }
}
