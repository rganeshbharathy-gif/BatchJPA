package com.example.batchjpa.config;

import com.example.batchjpa.entity.DataRecord;
import com.example.batchjpa.listener.RowCountValidationListener;
import com.example.batchjpa.model.DynamicDataRecord;
import com.example.batchjpa.processor.DynamicDataRecordProcessor;
import com.example.batchjpa.reader.DynamicLineMapper;
import com.example.batchjpa.tasklet.FileValidationTasklet;
import jakarta.persistence.EntityManagerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.batch.core.configuration.annotation.StepScope;

import java.util.regex.Pattern;

/**
 * Central Spring Batch 5 configuration.
 *
 * <h3>Job flow</h3>
 * <pre>
 *   fileValidationStep  ──(success)──►  dataLoadStep
 *         │                                  │
 *     (FAILED)                           (FAILED)
 *         │                                  │
 *       job FAILED                       job FAILED
 * </pre>
 *
 * <h3>Key design decisions</h3>
 * <ul>
 *   <li>{@code @EnableBatchProcessing} is intentionally absent – Spring Boot 3.x
 *       auto-configuration provides all necessary infrastructure beans, and
 *       combining the annotation with Boot causes duplicate-bean conflicts in
 *       Spring Batch 5.</li>
 *   <li>A single {@link JpaTransactionManager} is declared as the
 *       {@link PlatformTransactionManager}.  Spring Boot's Batch auto-config
 *       detects it and uses it for both the metadata {@link JobRepository} and
 *       every chunk step, ensuring that chunk writes and JPA session flushing
 *       share the same transaction boundary.</li>
 *   <li>The {@link FlatFileItemReader} is {@code @StepScope} so it can receive
 *       late-bound values ({@code file.path}, {@code dataLineCount},
 *       {@code headerLine}) from the {@code JobExecutionContext} populated by
 *       the validation tasklet.</li>
 *   <li>{@link JpaItemWriter#setUsePersist(boolean) setUsePersist(false)} forces
 *       {@code EntityManager.merge()} instead of {@code persist()}, giving
 *       upsert (insert-or-update) semantics for the daily-refresh use-case.</li>
 * </ul>
 */
@Configuration
public class BatchConfig {

    // =========================================================================
    // Transaction manager
    // =========================================================================

    /**
     * Registers a {@link JpaTransactionManager} as the application's primary
     * {@link PlatformTransactionManager}.
     *
     * <p>Spring Boot's {@code BatchAutoConfiguration} picks this up automatically
     * and uses it for the Spring Batch metadata tables as well as every
     * chunk step defined below.
     *
     * @param emf the auto-configured {@link EntityManagerFactory}
     * @return a fully initialised {@link JpaTransactionManager}
     */
    @Bean
    public PlatformTransactionManager transactionManager(EntityManagerFactory emf) {
        return new JpaTransactionManager(emf);
    }

    // =========================================================================
    // Job
    // =========================================================================

    /**
     * Top-level job definition.
     *
     * <p>Step 1 ({@code fileValidationStep}) must succeed for Step 2
     * ({@code dataLoadStep}) to run.  If the footer count mismatches the
     * actual data-row count, Step 1 throws an exception, the job is marked
     * FAILED, and <em>no records are written to the database</em>.
     *
     * @param jobRepository  Spring Batch job repository (auto-configured)
     * @param validationStep the file-validation tasklet step
     * @param dataLoadStep   the chunk-oriented data-load step
     * @return the configured {@link Job}
     */
    @Bean
    public Job dataLoadJob(JobRepository jobRepository,
                           Step fileValidationStep,
                           Step dataLoadStep) {
        return new JobBuilder("dataLoadJob", jobRepository)
                .start(fileValidationStep)
                .next(dataLoadStep)
                .build();
    }

    // =========================================================================
    // Step 1 – file validation (Tasklet)
    // =========================================================================

    /**
     * Tasklet step that validates the input file's footer row-count against the
     * actual number of data lines.  On mismatch it throws
     * {@link com.example.batchjpa.tasklet.FileValidationTasklet.FooterMismatchException},
     * preventing Step 2 from running.
     *
     * <p>The tasklet also extracts the header line and total data-line count
     * into the {@code JobExecutionContext} so the chunk reader can consume them
     * via {@code @StepScope} SpEL expressions.
     *
     * @param jobRepository        auto-configured job repository
     * @param txManager            the {@link JpaTransactionManager} declared above
     * @param fileValidationTasklet the {@code @StepScope} tasklet bean
     * @return the validation step
     */
    @Bean
    public Step fileValidationStep(JobRepository jobRepository,
                                   PlatformTransactionManager txManager,
                                   FileValidationTasklet fileValidationTasklet) {
        return new StepBuilder("fileValidationStep", jobRepository)
                .tasklet(fileValidationTasklet, txManager)
                .build();
    }

    // =========================================================================
    // Step 2 – chunk-oriented data load (local chunk processing)
    // =========================================================================

    /**
     * Chunk-oriented step that reads the flat file, maps dynamic columns, and
     * merges each record into the Oracle database via JPA.
     *
     * <p>Local chunk processing means the reader, processor, and writer all run
     * in the same JVM thread.  Each chunk of {@code batch.chunk.size} items is
     * wrapped in a single {@link PlatformTransactionManager} transaction – if
     * any item in the chunk fails, only that chunk is rolled back.
     *
     * @param jobRepository            auto-configured job repository
     * @param txManager                the {@link JpaTransactionManager}
     * @param itemReader               {@code @StepScope} flat-file reader
     * @param processor                item processor (DynamicDataRecord → DataRecord)
     * @param itemWriter               JPA writer configured for merge
     * @param rowCountValidationListener after-step row-count guard
     * @param chunkSize                resolved from {@code batch.chunk.size}
     * @return the data-load step
     */
    @Bean
    public Step dataLoadStep(JobRepository jobRepository,
                             PlatformTransactionManager txManager,
                             FlatFileItemReader<DynamicDataRecord> itemReader,
                             DynamicDataRecordProcessor processor,
                             JpaItemWriter<DataRecord> itemWriter,
                             RowCountValidationListener rowCountValidationListener,
                             @Value("${batch.chunk.size:500}") int chunkSize) {

        return new StepBuilder("dataLoadStep", jobRepository)
                .<DynamicDataRecord, DataRecord>chunk(chunkSize, txManager)
                .reader(itemReader)
                .processor(processor)
                .writer(itemWriter)
                .listener(rowCountValidationListener)
                .build();
    }

    // =========================================================================
    // Reader  (Step-scoped – receives late-bound JobExecutionContext values)
    // =========================================================================

    /**
     * Built-in {@link FlatFileItemReader} configured for dynamic-column files.
     *
     * <h3>How dynamic columns work</h3>
     * <ol>
     *   <li>The {@code FileValidationTasklet} (Step 1) reads the header and stores
     *       the raw header line in {@code JobExecutionContext['headerLine']}.</li>
     *   <li>This factory method is called when Step 2 starts ({@code @StepScope}
     *       bean creation).  It splits the header line on the configured delimiter
     *       to discover the ordered column names and their positional indexes.</li>
     *   <li>A {@link DynamicLineMapper} is constructed with those column names;
     *       it maps every subsequent data line by position.</li>
     * </ol>
     *
     * <h3>Footer exclusion</h3>
     * {@code linesToSkip(1)} skips the header; {@code maxItemCount(dataLineCount)}
     * stops reading after all data rows, so the footer line is never presented
     * to the mapper.
     *
     * @param filePath      resolved from {@code JobParameter['file.path']}
     * @param dataLineCount resolved from {@code JobExecutionContext['dataLineCount']}
     *                      (set by the validation tasklet)
     * @param headerLine    resolved from {@code JobExecutionContext['headerLine']}
     *                      (raw header string set by the validation tasklet)
     * @param delimiter     field delimiter, default {@code |}
     * @return a configured {@link FlatFileItemReader}
     */
    @Bean
    @StepScope
    public FlatFileItemReader<DynamicDataRecord> itemReader(
            @Value("#{jobParameters['file.path']}")          String filePath,
            @Value("#{jobExecutionContext['dataLineCount']}") Long   dataLineCount,
            @Value("#{jobExecutionContext['headerLine']}")    String headerLine,
            @Value("${batch.file.delimiter:|}")               String delimiter) {

        // Discover column names from the header line stored by Step 1
        String[] columnNames = headerLine.split(Pattern.quote(delimiter), -1);

        // Trim whitespace from each column name to handle files with padding
        for (int i = 0; i < columnNames.length; i++) {
            columnNames[i] = columnNames[i].trim();
        }

        DynamicLineMapper lineMapper = new DynamicLineMapper(columnNames, delimiter);

        return new FlatFileItemReaderBuilder<DynamicDataRecord>()
                .name("dynamicFileItemReader")
                .resource(new FileSystemResource(filePath))
                // Skip the header row (column names are already in columnNames[])
                .linesToSkip(1)
                // Stop before the footer: dataLineCount == totalLines - header - footer
                .maxItemCount(dataLineCount.intValue())
                .lineMapper(lineMapper)
                .build();
    }

    // =========================================================================
    // Writer
    // =========================================================================

    /**
     * Built-in {@link JpaItemWriter} configured to use
     * {@code EntityManager.merge()} instead of {@code persist()}.
     *
     * <h3>Daily-refresh (upsert) semantics</h3>
     * <ul>
     *   <li>If a {@code DataRecord} with the same {@code recordKey} already exists
     *       in {@code DATA_RECORD}, Hibernate issues an {@code UPDATE} and then
     *       replaces the {@code DATA_RECORD_ATTRIBUTE} rows (DELETE + INSERT).</li>
     *   <li>If the key is new, Hibernate issues an {@code INSERT} on both tables.</li>
     * </ul>
     * This is the standard JPA merge contract and requires no custom SQL or stored
     * procedures.
     *
     * @param emf the auto-configured {@link EntityManagerFactory}
     * @return a merge-mode {@link JpaItemWriter}
     */
    @Bean
    public JpaItemWriter<DataRecord> itemWriter(EntityManagerFactory emf) {
        JpaItemWriter<DataRecord> writer = new JpaItemWriter<>();
        writer.setEntityManagerFactory(emf);
        // false → EntityManager.merge()  (upsert / daily-refresh)
        // true  → EntityManager.persist() (insert-only, would fail on duplicates)
        writer.setUsePersist(false);
        return writer;
    }
}
