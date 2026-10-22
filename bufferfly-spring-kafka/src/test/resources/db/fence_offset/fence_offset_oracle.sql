-- FENCE_OFFSET — BufferFly per-partition consumer offset, used as an
-- optimistic-locking fencing token (advanced with a compare-and-set in the same
-- transaction as the data). Default schema expected by
-- io.bufferfly.spring.persistence.FencingOffsetRepository.
--
-- Oracle

CREATE TABLE FENCE_OFFSET (
    NAM_TOPIC     VARCHAR2(128)                      NOT NULL,
    NUM_PARTITION NUMBER(10)                         NOT NULL,
    NUM_OFFSET    NUMBER(19)                         NOT NULL,
    DAT_CREATED   TIMESTAMP DEFAULT SYSTIMESTAMP     NOT NULL,
    CONSTRAINT PK_FENCE_OFFSET PRIMARY KEY (NAM_TOPIC, NUM_PARTITION)
);
