package com.example.batchjpa.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.stereotype.Component;

/**
 * Secondary row-count guard for the data-load chunk step.
 *
 * <h3>Why two layers?</h3>
 * The primary guard is {@link com.example.batchjpa.tasklet.FileValidationTasklet}
 * which runs <em>before</em> any data is written and blocks the job if the
 * footer count is wrong.  This listener is a belt-and-braces second check that
 * runs <em>after</em> all chunks have been committed; it catches edge cases such
 * as:
 * <ul>
 *   <li>Skip policies that silently drop items after the tasklet validated the
 *       file (so fewer items than expected reach the writer).</li>
 *   <li>Future changes that introduce item filtering inside the processor.</li>
 * </ul>
 *
 * <h3>On mismatch</h3>
 * The listener returns {@link ExitStatus#FAILED}, which causes Spring Batch to
 * mark the step – and therefore the job – as FAILED.  Because all chunk
 * transactions have already been committed at this point the listener also logs
 * a prominent warning so that an operator can decide whether to truncate and
 * re-run.
 *
 * <p>If your recovery requirement is stricter (must roll back every committed
 * chunk), consider either:
 * <ul>
 *   <li>Writing to a staging table and promoting to the target table only in a
 *       final tasklet step that runs inside a single transaction, or</li>
 *   <li>Configuring a very large chunk size so the entire file fits in one
 *       transaction (only practical for small files).</li>
 * </ul>
 */
@Component
public class RowCountValidationListener implements StepExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(RowCountValidationListener.class);

    // -------------------------------------------------------------------------
    // StepExecutionListener – afterStep
    // -------------------------------------------------------------------------

    /**
     * Compares Spring Batch's own {@code writeCount} against the expected row
     * count stored in the {@code JobExecutionContext} by
     * {@link com.example.batchjpa.tasklet.FileValidationTasklet}.
     *
     * @param stepExecution the completed step execution
     * @return {@link ExitStatus#COMPLETED} when counts match;
     *         {@link ExitStatus#FAILED} when they diverge
     */
    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {

        long expected = stepExecution
                .getJobExecution()
                .getExecutionContext()
                .getLong("dataLineCount", -1L);

        long written = stepExecution.getWriteCount();

        if (expected < 0) {
            // FileValidationTasklet did not populate the context – step 1 may
            // have been skipped; treat as a configuration error
            log.error("'dataLineCount' not found in JobExecutionContext – cannot validate row count.");
            return ExitStatus.FAILED;
        }

        if (written != expected) {
            log.error(
                    "Row-count mismatch after data-load step: expected {} writes but Spring Batch "
                    + "recorded {} writes. The step will be marked FAILED. "
                    + "Check skip/filter configuration or manually reconcile the target table.",
                    expected, written);
            return ExitStatus.FAILED;
        }

        log.info("Row-count validation passed: {} records written (expected {}).", written, expected);
        return ExitStatus.COMPLETED;
    }
}
