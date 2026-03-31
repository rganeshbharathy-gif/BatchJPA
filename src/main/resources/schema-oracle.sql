-- ============================================================
--  DDL for BatchJPA application tables (Oracle)
--  Run once against the target schema before the first job run.
--  Spring Batch meta-tables are created automatically by
--  spring.batch.jdbc.initialize-schema=always (Oracle scripts
--  are bundled inside spring-batch-core.jar).
-- ============================================================

-- ------------------------------------------------------------
--  Main entity table
-- ------------------------------------------------------------
CREATE TABLE DATA_RECORD (
    RECORD_KEY   VARCHAR2(255 CHAR) NOT NULL,
    LOAD_DATE    DATE               NOT NULL,
    CONSTRAINT PK_DATA_RECORD PRIMARY KEY (RECORD_KEY)
);

COMMENT ON TABLE  DATA_RECORD            IS 'One row per unique record key; refreshed daily via merge.';
COMMENT ON COLUMN DATA_RECORD.RECORD_KEY IS 'Value of the first column in the input file (natural key).';
COMMENT ON COLUMN DATA_RECORD.LOAD_DATE  IS 'Date the record was last loaded / refreshed.';

-- ------------------------------------------------------------
--  Element-collection table – dynamic attributes
-- ------------------------------------------------------------
CREATE TABLE DATA_RECORD_ATTRIBUTE (
    RECORD_KEY  VARCHAR2(255 CHAR) NOT NULL,
    ATTR_NAME   VARCHAR2(255 CHAR) NOT NULL,
    ATTR_VALUE  VARCHAR2(4000 CHAR),
    CONSTRAINT PK_DATA_RECORD_ATTR PRIMARY KEY (RECORD_KEY, ATTR_NAME),
    CONSTRAINT FK_DRA_RECORD FOREIGN KEY (RECORD_KEY)
        REFERENCES DATA_RECORD (RECORD_KEY)
        ON DELETE CASCADE
);

COMMENT ON TABLE  DATA_RECORD_ATTRIBUTE           IS 'Key-value pairs for each dynamic column beyond the record key.';
COMMENT ON COLUMN DATA_RECORD_ATTRIBUTE.RECORD_KEY IS 'FK to DATA_RECORD.';
COMMENT ON COLUMN DATA_RECORD_ATTRIBUTE.ATTR_NAME  IS 'Column name from the input file header.';
COMMENT ON COLUMN DATA_RECORD_ATTRIBUTE.ATTR_VALUE IS 'Column value from the input file data row.';

-- ------------------------------------------------------------
--  Indexes for frequent query patterns
-- ------------------------------------------------------------
CREATE INDEX IDX_DRA_RECORD_KEY ON DATA_RECORD_ATTRIBUTE (RECORD_KEY);
