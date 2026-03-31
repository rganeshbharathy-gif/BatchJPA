package com.example.batchjpa.processor;

import com.example.batchjpa.entity.DataRecord;
import com.example.batchjpa.model.DynamicDataRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.HashMap;

/**
 * Converts a {@link DynamicDataRecord} DTO (produced by the reader) into a
 * {@link DataRecord} JPA entity ready for persistence.
 *
 * <h3>Responsibilities</h3>
 * <ul>
 *   <li>Copies the dynamic columns map into the entity's {@code attributes}
 *       collection.</li>
 *   <li>Stamps the current date as {@code loadDate} so queries can filter by
 *       the daily-refresh date.</li>
 *   <li>Returns {@code null} for any item that should be filtered out (Spring
 *       Batch will not pass {@code null} items to the writer).</li>
 * </ul>
 *
 * <h3>Daily-refresh behaviour</h3>
 * The processor always returns a fresh, detached {@link DataRecord}.  When the
 * {@code JpaItemWriter} calls {@code EntityManager.merge(record)}, Hibernate
 * will:
 * <ul>
 *   <li><b>INSERT</b> the row if the {@code recordKey} is new.</li>
 *   <li><b>UPDATE</b> the row (and replace the {@code @ElementCollection}
 *       attributes) if the key already exists in the database.</li>
 * </ul>
 * This gives "upsert" semantics without any native SQL, fulfilling the
 * daily-refresh requirement.
 */
@Component
public class DynamicDataRecordProcessor implements ItemProcessor<DynamicDataRecord, DataRecord> {

    private static final Logger log = LoggerFactory.getLogger(DynamicDataRecordProcessor.class);

    @Override
    public DataRecord process(DynamicDataRecord item) {

        if (item == null) {
            return null; // should never happen, but be defensive
        }

        // Guard: skip rows that somehow arrive with a blank key
        if (item.recordKey() == null || item.recordKey().isBlank()) {
            log.warn("Skipping row with blank record key; columns={}", item.columns());
            return null;
        }

        DataRecord entity = new DataRecord(
                item.recordKey(),
                LocalDate.now(),
                new HashMap<>(item.columns())
        );

        log.debug("Processed record: key={}, attributeCount={}",
                entity.getRecordKey(), entity.getAttributes().size());

        return entity;
    }
}
