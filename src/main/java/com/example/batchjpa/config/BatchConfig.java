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
     * Builds a {@link FlatFileItemReader} that:
     * <ul>
     *   <li>Opens the file at {@code file.path} (from job parameters).</li>
     *   <li>Skips line 1 (the header – column names were already captured by the
     *       tasklet).</li>
     *   <li>Sets {@code maxItemCount} to {@code dataLineCount} so the reader
     *       stops exactly before the footer line without reading it as a data
     *       row.</li>
     *   <li>Uses a {@link DynamicLineMapper} constructed from the raw
     *       {@code headerLine} stored in the {@code JobExecutionContext}.</li>
     * </ul>
     *
     * @param filePath      resolved from job parameter {@code file.path}
     * @param dataLineCount number of data rows, from {@code jobExecutionContext['dataLineCount']}
     * @param headerLine    raw header line text, from {@code jobExecutionContext['headerLine']}
     * @param delimiter     field separator, from application property {@code batch.file.delimiter}
     * @return a fully configured {@link FlatFileItemReader}
     */
    @Bean
    @StepScope
    public FlatFileItemReader<DynamicDataRecord> itemReader(
            @Value("#{jobParameters['file.path']}")          String filePath,
            @Value("#{jobExecutionContext['dataLineCount']}") Long   dataLineCount,
            @Value("#{jobExecutionContext['headerLine']}")    String headerLine,
            @Value("${batch.file.delimiter:|}")               String delimiter) {

        // Parse column names from the header line stored by FileValidationTasklet
        String[] columnNames = headerLine.split(Pattern.quote(delimiter), -1);

        // Trim any surrounding whitespace from each column name
        for (int i = 0; i < columnNames.length; i++) {
            columnNames[i] = columnNames[i].strip();
        }

        DynamicLineMapper lineMapper = new DynamicLineMapper(columnNames, delimiter);

        return new FlatFileItemReaderBuilder<DynamicDataRecord>()
                .name("dynamicFlatFileItemReader")
                .resource(new FileSystemResource(filePath))
                // Skip line 1: header was already consumed by the tasklet
                .linesToSkip(1)
                // Stop reading before the footer line
                .maxItemCount(dataLineCount.intValue())
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
     * {@code chunkSize} records via {@link JpaItemWriter}.
     *
     * <p>The {@link RowCountValidationListener} runs after the step completes as
     * a secondary safety net.
     *
     * @param jobRepository       auto-configured by Spring Boot
     * @param transactionManager  the {@link JpaTransactionManager} defined above
     * @param reader              the {@code @StepScope} flat-file reader
     * @param processor           the DTO-to-entity processor
     * @param writer              the JPA writer
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
            JpaItemWriter<DataRecord> writer,
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
