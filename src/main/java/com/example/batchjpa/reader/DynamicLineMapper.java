package com.example.batchjpa.reader;

import com.example.batchjpa.model.DynamicDataRecord;
import org.springframework.batch.item.file.LineMapper;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Stateless {@link LineMapper} that extracts a <em>selected subset</em> of columns
 * from a wide delimited line and maps them to a {@link DynamicDataRecord}.
 *
 * <p>This class is a plain POJO – not a Spring component – because it is
 * constructed programmatically inside the {@code @StepScope} reader bean in
 * {@link com.example.batchjpa.config.BatchConfig}, where the column names,
 * their source indexes, and the delimiter are already resolved.
 *
 * <h3>Why indexes instead of sequential iteration</h3>
 * The input file may have 150 (or any number of) columns, but only a small
 * subset is needed.  {@code columnIndexes[i]} holds the <em>position in the
 * raw token array</em> for the {@code i}-th target column.  The mapper jumps
 * directly to each position, leaving the other 146 tokens untouched.
 *
 * <pre>
 * Header line (150 cols):  col0|col1|...|CSI-ID|...|Grid id|...|eco sector code|...|country code|...
 *                                          ↑ idx=7     ↑ idx=23       ↑ idx=51           ↑ idx=88
 *
 * columnNames   = ["CSI-ID", "Grid id", "eco sector code", "country code"]
 * columnIndexes = [       7,        23,                51,             88]
 * </pre>
 *
 * <h3>Design notes</h3>
 * <ul>
 *   <li>The first target column value (e.g. {@code CSI-ID}) becomes
 *       {@code recordKey} – the entity PK used for daily-refresh merge.</li>
 *   <li>Only the 4 selected columns are placed in the {@code columns} map;
 *       the remaining 146 are never allocated.</li>
 *   <li>If a data row has fewer tokens than the highest required index the
 *       missing value is stored as an empty string.</li>
 * </ul>
 *
 * @param columnNames   ordered array of target column names (e.g. 4 names)
 * @param columnIndexes parallel array of source positions in the raw token array
 * @param delimiter     field separator character(s), e.g. {@code "|"}
 */
public record DynamicLineMapper(
        String[] columnNames,
        int[]    columnIndexes,
        String   delimiter
) implements LineMapper<DynamicDataRecord> {

    /**
     * Compact canonical constructor: validates and defensively copies all arrays
     * so that external mutations after construction have no effect.
     */
    public DynamicLineMapper {
        if (columnNames == null || columnNames.length == 0) {
            throw new IllegalArgumentException("columnNames must not be null or empty");
        }
        if (columnIndexes == null || columnIndexes.length == 0) {
            throw new IllegalArgumentException("columnIndexes must not be null or empty");
        }
        if (columnNames.length != columnIndexes.length) {
            throw new IllegalArgumentException(
                    "columnNames length (%d) must match columnIndexes length (%d)"
                            .formatted(columnNames.length, columnIndexes.length));
        }
        if (delimiter == null || delimiter.isEmpty()) {
            throw new IllegalArgumentException("delimiter must not be null or empty");
        }
        columnNames   = columnNames.clone();   // defensive copy
        columnIndexes = columnIndexes.clone(); // defensive copy
    }

    /**
     * Parses {@code line} by jumping to each target column's source index.
     *
     * <p>Only {@code columnNames.length} map entries are created regardless of
     * how many pipe-separated tokens the raw line contains.
     *
     * @param line       raw text line from the flat file
     * @param lineNumber 1-based line number (used only in error messages)
     * @return a {@link DynamicDataRecord} containing only the selected columns
     */
    @Override
    public DynamicDataRecord mapLine(String line, int lineNumber) {

        if (line == null || line.isBlank()) {
            throw new IllegalArgumentException(
                    "Blank line at line %d – check maxItemCount / linesToSkip settings"
                            .formatted(lineNumber));
        }

        // Split the full 150-column line on the literal delimiter.
        // -1 limit preserves trailing empty fields.
        String[] tokens = line.split(java.util.regex.Pattern.quote(delimiter), -1);

        Map<String, String> columns = LinkedHashMap.newLinkedHashMap(columnNames.length);

        for (int i = 0; i < columnNames.length; i++) {
            int    srcIdx = columnIndexes[i];                              // jump to source position
            String value  = (srcIdx < tokens.length) ? tokens[srcIdx].trim() : "";
            columns.put(columnNames[i], value);
        }

        // First target column (CSI-ID) is the record key for JPA merge
        String recordKey = columns.get(columnNames[0]);

        return new DynamicDataRecord(recordKey, columns);
    }
}
