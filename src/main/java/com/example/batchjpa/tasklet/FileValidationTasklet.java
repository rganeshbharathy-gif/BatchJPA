package com.example.batchjpa.tasklet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.batch.core.configuration.annotation.StepScope;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Step 1 tasklet – validates the input flat file before any data is written.
 *
 * <h3>What it does</h3>
 * <ol>
 *   <li>Streams through the file once to count lines, capture the header and footer.</li>
 *   <li>Parses the footer as a {@code long} (expected row count).</li>
 *   <li>Derives the actual data-row count as {@code totalLines - 2}
 *       (subtracting the header and footer lines).</li>
 *   <li>If the footer count does not match the actual data-row count it throws a
 *       {@link FooterMismatchException}, which Spring Batch propagates as a step/job
 *       failure <em>before</em> any records have been written to the database –
 *       effectively acting as a pre-write rollback guard.</li>
 *   <li>On success it stores two keys into the {@link ExecutionContext} so
 *       the chunk step can consume them via {@code @StepScope} / SpEL:
 *       <ul>
 *         <li>{@code dataLineCount} – number of data rows to read</li>
 *         <li>{@code headerLine}    – raw header line (column names)</li>
 *       </ul>
 *   </li>
 * </ol>
 *
 * <h3>Memory profile</h3>
 * Only three {@code String} variables are held in memory at any point regardless
 * of file size – the current line, the first line, and the rolling "last seen"
 * line.  The file is never fully loaded into memory.
 */
@Component
@StepScope
public class FileValidationTasklet implements Tasklet {

    private static final Logger log = LoggerFactory.getLogger(FileValidationTasklet.class);

    /** Injected from the JobParameter {@code file.path}. */
    @Value("#{jobParameters['file.path']}")
    private String filePath;

    @Value("${batch.file.delimiter:|}")
    private String delimiter;

    // -------------------------------------------------------------------------
    // Tasklet contract
    // -------------------------------------------------------------------------

    @Override
    public RepeatStatus execute(StepContribution contribution,
                                ChunkContext chunkContext) throws Exception {

        Path path = Path.of(filePath);

        if (!Files.exists(path)) {
            throw new FileNotFoundException("Input file not found: " + filePath);
        }
        if (!Files.isReadable(path)) {
            throw new IllegalStateException("Input file is not readable: " + filePath);
        }

        log.info("Validating input file: {}", path.toAbsolutePath());

        // ------------------------------------------------------------------
        // Single-pass streaming read: track first line, last non-blank line,
        // and a running total.  This keeps memory usage O(1).
        // ------------------------------------------------------------------
        long totalLines = 0;
        String headerLine = null;
        String footerLine = null;

        try (BufferedReader br = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            String line;
            while ((line = br.readLine()) != null) {
                totalLines++;
                if (totalLines == 1) {
                    headerLine = line;   // capture header
                }
                if (!line.isBlank()) {
                    footerLine = line;   // always points to last non-blank line
                }
            }
        }

        log.debug("Total lines read (including header + footer): {}", totalLines);

        // ------------------------------------------------------------------
        // Structural guards
        // ------------------------------------------------------------------
        if (totalLines < 3) {
            throw new IllegalStateException(
                    "File '%s' has only %d line(s); it must contain at least a header, "
                    .formatted(filePath, totalLines)
                    + "one data row, and a footer.");
        }

        if (headerLine == null || headerLine.isBlank()) {
            throw new IllegalStateException("Header line (line 1) is blank in: " + filePath);
        }

        // ------------------------------------------------------------------
        // Parse footer
        // ------------------------------------------------------------------
        long expectedRowCount;
        try {
            assert footerLine != null;
            expectedRowCount = Long.parseLong(footerLine.strip());
        } catch (NumberFormatException e) {
            throw new IllegalStateException(
                    "Footer line is not a valid integer: '%s' in file: %s"
                    .formatted(footerLine, filePath), e);
        }

        long actualDataLines = totalLines - 2; // exclude header and footer

        log.info("Footer declares {} rows; file contains {} data rows.", expectedRowCount, actualDataLines);

        // ------------------------------------------------------------------
        // Core validation – mismatch → fail BEFORE any writes occur
        // ------------------------------------------------------------------
        if (expectedRowCount != actualDataLines) {
            throw new FooterMismatchException(
                    ("Footer count mismatch in file '%s': footer declares %d rows "
                     + "but the file has %d data rows. "
                     + "No records have been written – transaction rolled back.")
                    .formatted(filePath, expectedRowCount, actualDataLines));
        }

        // ------------------------------------------------------------------
        // Validate header has at least one column
        // ------------------------------------------------------------------
        String[] columnNames = headerLine.split(java.util.regex.Pattern.quote(delimiter), -1);
        if (columnNames.length == 0 || (columnNames.length == 1 && columnNames[0].isBlank())) {
            throw new IllegalStateException(
                    "Header line produced no column names with delimiter '%s': %s"
                    .formatted(delimiter, headerLine));
        }

        // ------------------------------------------------------------------
        // Publish results to the JobExecutionContext so the chunk step can
        // consume them via @Value("#{jobExecutionContext['...']}")
        // ------------------------------------------------------------------
        ExecutionContext jobCtx = chunkContext.getStepContext()
                .getStepExecution()
                .getJobExecution()
                .getExecutionContext();

        jobCtx.putLong("dataLineCount", actualDataLines);
        jobCtx.putString("headerLine",  headerLine);

        log.info("File validation passed: {} data rows, {} columns detected: {}",
                actualDataLines, columnNames.length, headerLine);

        return RepeatStatus.FINISHED;
    }

    // -------------------------------------------------------------------------
    // Nested exception – gives callers a specific type to catch/handle
    // -------------------------------------------------------------------------

    /**
     * Thrown when the footer row-count does not match the number of data lines
     * in the file.  Extends {@link RuntimeException} so Spring Batch marks the
     * step as FAILED and rolls back / prevents the next step from executing.
     */
    public static final class FooterMismatchException extends RuntimeException {
        public FooterMismatchException(String message) {
            super(message);
        }
    }
}
