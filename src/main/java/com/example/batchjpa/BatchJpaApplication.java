package com.example.batchjpa;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

/**
 * Spring Boot entry point for the BatchJPA application.
 *
 * <p>The job is NOT launched automatically by Spring Batch
 * ({@code spring.batch.job.enabled=false} in application.properties).
 * Instead, {@link ApplicationRunner} launches it programmatically so that
 * callers can pass any required {@link JobParameters} (e.g. via CLI args or
 * an orchestration layer).
 *
 * <p>Usage (fat-jar):
 * <pre>
 *   java --enable-preview -jar batch-jpa.jar \
 *        --file.path=/data/daily-feed.txt \
 *        --run.id=$(date +%s)
 * </pre>
 *
 * <p>Note: {@code @EnableBatchProcessing} is intentionally omitted – Spring
 * Boot 3.x auto-configuration provides equivalent setup, and mixing the two
 * causes duplicate bean conflicts in Spring Batch 5.
 */
@SpringBootApplication
public class BatchJpaApplication {

    public static void main(String[] args) {
        SpringApplication.run(BatchJpaApplication.class, args);
    }

    /**
     * Launches the {@code dataLoadJob} once on startup, resolving the input
     * file path from the {@code --file.path} command-line argument.
     *
     * <p>A unique {@code run.id} parameter is appended automatically so the
     * same file can be re-processed on subsequent days without Spring Batch
     * treating it as a duplicate job instance.
     */
    @Bean
    ApplicationRunner jobRunner(JobLauncher jobLauncher, Job dataLoadJob) {
        return (ApplicationArguments args) -> {

            // Resolve file path: --file.path=... or fall back to sample file
            String filePath = args.containsOption("file.path")
                    ? args.getOptionValues("file.path").getFirst()
                    : "src/main/resources/sample-input.txt";

            JobParameters params = new JobParametersBuilder()
                    .addString("file.path", filePath)
                    // Unique run ID ensures a new JobInstance each execution
                    .addLong("run.id", System.currentTimeMillis())
                    .toJobParameters();

            var execution = jobLauncher.run(dataLoadJob, params);

            System.out.printf(
                    "%n[BatchJPA] Job finished – status: %s, exitCode: %s%n",
                    execution.getStatus(),
                    execution.getExitStatus().getExitCode());
        };
    }
}
