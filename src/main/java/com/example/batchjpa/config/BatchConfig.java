package com.example.batchjpa.config;

import com.example.batchjpa.entity.DataRecord;
import com.example.batchjpa.listener.RowCountValidationListener;
import com.example.batchjpa.model.DynamicDataRecord;
import com.example.batchjpa.processor.DynamicDataRecordProcessor;
import com.example.batchjpa.reader.DynamicLineMapper;
import com.example.batchjpa.tasklet.FileValidationTasklet;
import com.example.batchjpa.writer.EntityManagerMergeWriter;

import jakarta.persistence.EntityManagerFactory;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.core.io.FileSystemResource;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Central Spring Batch configuration class.
 *
 * <p><strong>Architecture overview</strong>
 * <pre>
 *  dataLoadJob
 *   └─ Step 1: fileValidationStep   (FileValidationTasklet)
 *        • Streams the file to count lines, capture header/footer
 *        • Validates footer == actual data rows
 *        • Stores dataLineCount + headerLine in JobExecutionContext
 *        • Throws on mismatch → job FAILED before any writes
 *
 *   └─ Step 2: dataLoadStep         (chunk-oriented, chunk size = ${batch.chunk.size})
 *        • FlatFileItemReader  – skips header, stops before footer
 *        • DynamicLineMapper   – maps each line using header column names
 *        • DynamicDataRecordProcessor – converts DTO → JPA entity, stamps loadDate
 *        • JpaItemWriter       – EntityManager.merge() → upsert (usePersist=false)
 *        • RowCountValidationListener – afterStep secondary row-count guard
 * </pre>
 *
 * <p><strong>Spring Batch 5 notes</strong>
 * <ul>
 *   <li>{@code @EnableBatchProcessing} is intentionally absent – Spring Boot
 *       3.x auto-configures a {@code JobRepository} and related infrastructure
 *       automatically, and mixing both causes duplicate-bean conflicts.</li>
 *   <li>{@link JobBuilder} / {@link StepBuilder} replace the deprecated
 *       {@code JobBuilderFactory} / {@code StepBuilderFactory}.</li>
 *   <li>The {@link JpaTransactionManager} @Bean ensures that Hibernate's
 *       {@link EntityManagerFactory} drives all step transactions, not
 *       {@code DataSourceTransactionManager}.</li>
 * </ul>
 */
@Configuration
public class BatchConfig {

    // =========================================================================
    // Transaction manager
    // =========================================================================

    /**
     * Registers a {@link JpaTransactionManager} as the primary
     * {@link PlatformTransactionManager}.
     *
     * <p>Spring Boot auto-configures a {@code DataSourceTransactionManager} by
     * default; overriding it with {@code JpaTransactionManager} ensures that
     * Hibernate's first-level cache and flush semantics are respected during
     * the chunk step, and that the Batch meta-table updates participate in the
     * same transaction as the entity writes.
     *
     * @param emf the auto-configured {@link EntityManagerFactory}
     * @return a {@link JpaTransactionManager} wrapping the given factory
     */
    @Bean
    public PlatformTransactionManager transactionManager(EntityManagerFactory emf) {
        return new JpaTransactionManager(emf);
    }

    // =========================================================================
    // Reader  (@StepScope – resolved fresh per step execution)
    // =========================================================================

    /**
     * Builds a {@link FlatFileItemReader} that reads only the 4 target columns
     * from a wide (e.g. 150-column) pipe-delimited flat file.
     *
     * <h3>How column-index discovery works</h3>
     * <ol>
     *   <li>Split the raw {@code headerLine} (saved by {@link com.example.batchjpa.tasklet.FileValidationTasklet})
     *       on the delimiter to get all column names with their positions
     *       ({@code allColumnNames[i]} is at index {@code i}).</li>
     *   <li>Build a {@code name → index} lookup map from that array.</li>
     *   <li>For each name in {@code batch.file.target.columns}, look up its
     *       position.  Throw {@link IllegalStateException} if a required column
     *       is absent so the job fails fast in Step 1 with a clear message.</li>
     *   <li>Pass only the 4 target names + their source indexes to
     *       {@link DynamicLineMapper}.  Every data line is then split once into
     *       tokens and the mapper jumps straight to each required index —
     *       the other 146 values are never touched.</li>
     * </ol>
     *
     * <pre>
     * Header (150 cols): ...| CSI-ID |...| Grid id |...| eco sector code |...| country code |...
     *                           ↑ idx 7       ↑ idx 23         ↑ idx 51            ↑ idx 88
     *
     * columnNames   = ["CSI-ID",  "Grid id",  "eco sector code",  "country code"]
     * columnIndexes = [        7,          23,                51,              88]
     * </pre>
     *
     * <h3>Reader configuration</h3>
     * <ul>
     *   <li>{@code linesToSkip(1)} – skips the header line (already consumed by the tasklet).</li>
     *   <li>{@code maxItemCount(dataLineCount)} – stops the reader exactly before the footer
     *       line so it is never read as a data row.</li>
     * </ul>
     *
     * @param filePath           resolved from job parameter {@code file.path}
     * @param dataLineCount      data-row count, from {@code jobExecutionContext['dataLineCount']}
     * @param headerLine         raw header text, from {@code jobExecutionContext['headerLine']}
     * @param delimiter          field separator, from {@code batch.file.delimiter}
     * @param targetColumnsConfig comma-separated target column names, from {@code batch.file.target.columns}
     * @return a fully configured {@link FlatFileItemReader}
     */
    @Bean
    @StepScope
    public FlatFileItemReader<DynamicDataRecord> itemReader(
            @Value("#{jobParameters['file.path']}")          String filePath,
            @Value("#{jobExecutionContext['dataLineCount']}") Long   dataLineCount,
            @Value("#{jobExecutionContext['headerLine']}")    String headerLine,
            @Value("${batch.file.delimiter:|}")               String delimiter,
            @Value("${batch.file.target.columns}")            String targetColumnsConfig) {

        // ── Step 1: build index map for ALL columns in the file header ────────
        // e.g. {"col0"→0, "col1"→1, ..., "CSI-ID"→7, ..., "Grid id"→23, ...}
        String[] allColumnNames = headerLine.split(Pattern.quote(delimiter), -1);
        Map<String, Integer> headerIndexMap = new LinkedHashMap<>(allColumnNames.length);
        for (int i = 0; i < allColumnNames.length; i++) {
            headerIndexMap.put(allColumnNames[i].strip(), i);
        }

        // ── Step 2: find the source index of each required target column ──────
        // Configured via batch.file.target.columns=CSI-ID,Grid id,eco sector code,country code
        String[] targetNames   = targetColumnsConfig.split(",", -1);
        int[]    targetIndexes = new int[targetNames.length];

        for (int i = 0; i < targetNames.length; i++) {
            String col = targetNames[i].strip();
            Integer idx = headerIndexMap.get(col);
            if (idx == null) {
                throw new IllegalStateException(
                        ("Required column '%s' not found in file header. "
                         + "Available columns (%d): %s")
                                .formatted(col, allColumnNames.length, headerIndexMap.keySet()));
            }
            targetNames[i]   = col;   // store trimmed name
            targetIndexes[i] = idx;   // store its position in the 150-column line
        }

        // ── Step 3: build the mapper with only the 4 target names + indexes ───
        // mapLine() will call tokens[targetIndexes[i]] instead of tokens[i]
        DynamicLineMapper lineMapper = new DynamicLineMapper(targetNames, targetIndexes, delimiter);

        return new FlatFileItemReaderBuilder<DynamicDataRecord>()
                .name("dynamicFlatFileItemReader")
                .resource(new FileSystemResource(filePath))
                .linesToSkip(1)                        // skip header (already stored in jobExecutionContext)
                .maxItemCount(dataLineCount.intValue()) // stop before the footer line
                .lineMapper(lineMapper)
                .build();
    }

    // =========================================================================
    // Writer
    // =========================================================================

    /**
     * Builds a {@link JpaItemWriter} that calls
     * {@code EntityManager.merge(entity)} for each item.
     *
     * <p>{@code setUsePersist(false)} activates merge mode, which implements the
     * daily-refresh "upsert" contract:
     * <ul>
     *   <li>If the {@code recordKey} already exists in the database the row
     *       (and its {@code @ElementCollection} attributes) is updated.</li>
     *   <li>If it does not exist a new row is inserted.</li>
     * </ul>
     *
     * @param emf the auto-configured {@link EntityManagerFactory}
     * @return a configured {@link JpaItemWriter} for {@link DataRecord} entities
     */
    @Bean
    public JpaItemWriter<DataRecord> itemWriter(EntityManagerFactory emf) {
        JpaItemWriter<DataRecord> writer = new JpaItemWriter<>();
        writer.setEntityManagerFactory(emf);
        writer.setUsePersist(false); // merge → upsert for daily refresh
        return writer;
    }

    // =========================================================================
    // Steps
    // =========================================================================

    /**
     * Step 1 – runs {@link FileValidationTasklet} to validate the file.
     *
     * <p>If validation fails the tasklet throws, Spring Batch marks this step
     * FAILED, and the job stops without reaching Step 2.
     *
     * @param jobRepository      auto-configured by Spring Boot
     * @param transactionManager the {@link JpaTransactionManager} defined above
     * @param tasklet            the validation tasklet (injected by Spring)
     * @return the configured validation step
     */
    @Bean
    public Step fileValidationStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            FileValidationTasklet tasklet) {

        return new StepBuilder("fileValidationStep", jobRepository)
                .tasklet(tasklet, transactionManager)
                .build();
    }

    /**
     * Step 2 – chunk-oriented data-load step.
     *
     * <p>Reads up to {@code dataLineCount} lines from the file, processes each
     * through {@link DynamicDataRecordProcessor}, and writes batches of
     * {@code chunkSize} records via {@link EntityManagerMergeWriter} (Approach 2).
     *
     * <p><strong>Switching writers:</strong> To revert to Approach 1 ({@link JpaItemWriter}),
     * change the {@code writer} parameter type back to {@code JpaItemWriter<DataRecord>}.
     * The {@code itemWriter()} bean below remains available for that purpose.
     *
     * <p>The {@link RowCountValidationListener} runs after the step completes as
     * a secondary safety net.
     *
     * @param jobRepository       auto-configured by Spring Boot
     * @param transactionManager  the {@link JpaTransactionManager} defined above
     * @param reader              the {@code @StepScope} flat-file reader
     * @param processor           the DTO-to-entity processor
     * @param writer              Approach 2: custom EM writer (swap to {@code JpaItemWriter<DataRecord>} for Approach 1)
     * @param listener            the post-step row-count validator
     * @param chunkSize           from application property {@code batch.chunk.size} (default 500)
     * @return the configured data-load step
     */
    @Bean
    public Step dataLoadStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            FlatFileItemReader<DynamicDataRecord> reader,
            DynamicDataRecordProcessor processor,
            EntityManagerMergeWriter writer,           // Approach 2 — swap to JpaItemWriter<DataRecord> for Approach 1
            RowCountValidationListener listener,
            @Value("${batch.chunk.size:500}") int chunkSize) {

        return new StepBuilder("dataLoadStep", jobRepository)
                .<DynamicDataRecord, DataRecord>chunk(chunkSize, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .listener(listener)
                .build();
    }

    // =========================================================================
    // Job
    // =========================================================================

    /**
     * The main batch job: {@code fileValidationStep} → {@code dataLoadStep}.
     *
     * <p>The sequential flow ensures the file is fully validated before a single
     * record is written to the database.
     *
     * @param jobRepository      auto-configured by Spring Boot
     * @param fileValidationStep Step 1 (tasklet)
     * @param dataLoadStep       Step 2 (chunk)
     * @return the fully wired {@link Job}
     */
    @Bean
    public Job dataLoadJob(
            JobRepository jobRepository,
            Step fileValidationStep,
            Step dataLoadStep) {

        return new JobBuilder("dataLoadJob", jobRepository)
                .start(fileValidationStep)
                .next(dataLoadStep)
                .build();
    }
}
