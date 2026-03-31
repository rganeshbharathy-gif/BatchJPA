package com.example.batchjpa.writer;

import com.example.batchjpa.entity.DataRecord;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

/**
 * Approach 2: Custom {@link ItemWriter} that calls {@code EntityManager.merge()} explicitly.
 *
 * <h3>How it participates in the chunk transaction</h3>
 * {@code @PersistenceContext} injects a <em>container-managed EM proxy</em>, not a raw
 * {@link EntityManager}. The proxy transparently binds to the active Spring transaction
 * that {@code JpaTransactionManager} opened for the current chunk — so every
 * {@code merge()}, {@code flush()}, and {@code clear()} call executes within that
 * single chunk transaction. No {@code @Transactional} is needed here.
 *
 * <h3>Merge semantics (same as Approach 1)</h3>
 * <ul>
 *   <li>{@code recordKey} <strong>not</strong> in DB →
 *       {@code INSERT} into {@code DATA_RECORD} +
 *       {@code INSERT} into {@code DATA_RECORD_ATTRIBUTE}</li>
 *   <li>{@code recordKey} <strong>already</strong> in DB →
 *       {@code UPDATE} {@code DATA_RECORD} +
 *       {@code DELETE} all rows from {@code DATA_RECORD_ATTRIBUTE} for that key,
 *       then {@code INSERT} fresh rows (Hibernate orphan-removal on
 *       {@code @ElementCollection}).</li>
 * </ul>
 * This produces identical SQL to {@link org.springframework.batch.item.database.JpaItemWriter}
 * with {@code setUsePersist(false)}.
 *
 * <h3>Extra capabilities over Approach 1</h3>
 * <ul>
 *   <li>Insert vs update detection per item via {@code em.find()} before {@code merge()}.</li>
 *   <li>Per-chunk metrics logging (inserted / updated counts).</li>
 *   <li>Full control over flush and clear timing (e.g., flush every N items for huge chunks).</li>
 *   <li>Ability to add custom pre/post-merge logic per record.</li>
 * </ul>
 *
 * <h3>Switching between Approach 1 and Approach 2</h3>
 * In {@code BatchConfig.dataLoadStep()}, change the {@code writer} parameter type:
 * <pre>
 * // Approach 1 — built-in JpaItemWriter:
 * JpaItemWriter&lt;DataRecord&gt; writer
 *
 * // Approach 2 — this custom writer:
 * EntityManagerMergeWriter writer
 * </pre>
 * The {@code JpaItemWriter} bean in {@code BatchConfig.itemWriter()} is intentionally
 * kept so Approach 1 can be restored with a single-line change.
 */
@Component
public class EntityManagerMergeWriter implements ItemWriter<DataRecord> {

    private static final Logger log = LoggerFactory.getLogger(EntityManagerMergeWriter.class);

    /**
     * Container-managed EM proxy bound to the active chunk transaction.
     * Spring resolves the correct {@link EntityManager} instance per thread/transaction.
     */
    @PersistenceContext
    private EntityManager em;

    /**
     * Merges each {@link DataRecord} in the chunk into the database.
     *
     * <p>Flow per item:
     * <ol>
     *   <li>{@code em.find()} — SELECT by PK to detect insert vs update.</li>
     *   <li>{@code em.merge()} — upsert the entity and replace its
     *       {@code @ElementCollection} attributes.</li>
     * </ol>
     * After all items: {@code flush()} pushes pending SQL, {@code clear()} detaches
     * all entities to keep the persistence context memory-bounded across large files.
     *
     * @param chunk the current chunk of processed {@link DataRecord} entities
     */
    @Override
    public void write(Chunk<? extends DataRecord> chunk) {
        int inserted = 0;
        int updated  = 0;

        for (DataRecord record : chunk.getItems()) {
            // SELECT by PK — null means this key does not exist yet (new insert)
            boolean isNew = (em.find(DataRecord.class, record.getRecordKey()) == null);

            // merge() = INSERT if new, UPDATE + orphan-removal on @ElementCollection if existing
            em.merge(record);

            if (isNew) inserted++;
            else       updated++;
        }

        // Flush all pending INSERT/UPDATE/DELETE SQL to Oracle within the chunk transaction.
        // JpaTransactionManager will commit (or roll back) this transaction after write() returns.
        em.flush();

        // Clear the persistence context — detaches all entities.
        // Critical for large files: prevents unbounded memory growth across chunks.
        em.clear();

        log.info("Chunk written — inserted: {}, updated: {}, total: {}",
                 inserted, updated, chunk.size());
    }
}
