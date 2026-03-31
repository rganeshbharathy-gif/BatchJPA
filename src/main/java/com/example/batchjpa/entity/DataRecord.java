package com.example.batchjpa.entity;

import jakarta.persistence.CollectionTable;
import jakarta.persistence.Column;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.MapKeyColumn;
import jakarta.persistence.Table;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

/**
 * JPA entity representing one row from the daily input file.
 *
 * <p>The primary key ({@code recordKey}) is the value of the <em>first</em>
 * column in the file.  All remaining columns are stored as a dynamic
 * key-value map in the {@code DATA_RECORD_ATTRIBUTE} collection table,
 * enabling the schema to remain fixed regardless of how many columns the
 * input file contains.
 *
 * <p>Merge semantics ({@code JpaItemWriter#setUsePersist(false)}) implement
 * the daily-refresh "upsert" pattern:
 * <ul>
 *   <li>If the row already exists it is <em>updated</em> in-place.</li>
 *   <li>If it is new it is <em>inserted</em>.</li>
 * </ul>
 */
@Entity
@Table(name = "DATA_RECORD")
public class DataRecord {

    /** Natural key – first column value from the input file. */
    @Id
    @Column(name = "RECORD_KEY", nullable = false, length = 255)
    private String recordKey;

    /** Date this record was loaded/refreshed. Set by the processor. */
    @Column(name = "LOAD_DATE", nullable = false)
    private LocalDate loadDate;

    /**
     * Dynamic attributes: remaining column-name → column-value pairs.
     *
     * <p>{@link FetchType#EAGER} is used deliberately here because the batch
     * writer calls {@code EntityManager.merge()}, which needs the full state
     * of the managed entity to detect dirty changes before it flushes.
     */
    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(
            name = "DATA_RECORD_ATTRIBUTE",
            joinColumns = @JoinColumn(name = "RECORD_KEY")
    )
    @MapKeyColumn(name = "ATTR_NAME",  length = 255)
    @Column(     name = "ATTR_VALUE", length = 4000)
    private Map<String, String> attributes = new HashMap<>();

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /** Required no-arg constructor for JPA. */
    protected DataRecord() {}

    /**
     * Full constructor used by {@link com.example.batchjpa.processor.DynamicDataRecordProcessor}.
     *
     * @param recordKey  natural key (first column value)
     * @param loadDate   processing date
     * @param attributes remaining column-name → column-value pairs
     */
    public DataRecord(String recordKey, LocalDate loadDate, Map<String, String> attributes) {
        this.recordKey  = recordKey;
        this.loadDate   = loadDate;
        this.attributes = new HashMap<>(attributes);
    }

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

    public String getRecordKey() {
        return recordKey;
    }

    public void setRecordKey(String recordKey) {
        this.recordKey = recordKey;
    }

    public LocalDate getLoadDate() {
        return loadDate;
    }

    public void setLoadDate(LocalDate loadDate) {
        this.loadDate = loadDate;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    // -------------------------------------------------------------------------
    // Object overrides
    // -------------------------------------------------------------------------

    @Override
    public String toString() {
        return "DataRecord{recordKey='%s', loadDate=%s, attributes=%s}"
                .formatted(recordKey, loadDate, attributes);
    }
}
