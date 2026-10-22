-- FENCE_OFFSET — BufferFly per-partition consumer offset, used as an
-- optimistic-locking fencing token (advanced with a compare-and-set in the same
-- transaction as the data). Default schema expected by
-- io.bufferfly.spring.persistence.FencingOffsetRepository.
--
-- MySQL (InnoDB) — InnoDB is required for transactional CAS semantics.

CREATE TABLE FENCE_OFFSET (
    NAM_TOPIC     VARCHAR(128) NOT NULL,
    NUM_PARTITION INT          NOT NULL,
    NUM_OFFSET    BIGINT       NOT NULL,
    DAT_CREATED   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT PK_FENCE_OFFSET PRIMARY KEY (NAM_TOPIC, NUM_PARTITION)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
