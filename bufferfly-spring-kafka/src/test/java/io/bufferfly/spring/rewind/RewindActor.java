package io.bufferfly.spring.rewind;

import io.bufferfly.core.persistence.AbstractPersistentActor;
import io.bufferfly.core.persistence.PersistenceConfig;
import io.bufferfly.core.persistence.PersistenceRunner;
import io.bufferfly.spring.ConsumedRecord;
import io.bufferfly.spring.KafkaContext;

import java.util.List;

/**
 * A persistent, per-partition actor for the two-partition mid-batch
 * restart/rewind test. It persists each consumed record (idempotently by
 * {@code (partition, offset)}) and, on {@link #preStart()}, seeks to the next
 * offset after the one durably stored in the DB — so a fresh instance created by
 * a restart resumes exactly where the previous owner left off.
 *
 * <p>For observability the actor records, in order, the {@code (offset -> value)}
 * of every record handed to {@link #apply} on the actor thread. A restart/rewind
 * will therefore show the replayed offsets appearing a second time in this list
 * (the in-flight slice that was discarded + redelivered), while the DB stays
 * exactly-once thanks to the idempotent insert.
 */
public final class RewindActor extends AbstractPersistentActor<ConsumedRecord<String>, RewindMessage> {

    private final String name;
    private final int partition;
    private final KafkaContext ctx;
    private final RewindOperations operations;

    /** Every (offset,value) this actor instance applied, in order (actor-thread only, copy-on-write for the test to read). */
    private final List<String> applied;

    public RewindActor(String name,
                       int partition,
                       KafkaContext ctx,
                       RewindOperations operations,
                       PersistenceConfig<RewindMessage> config,
                       List<String> appliedSink) {
        super(config);
        this.name = name;
        this.partition = partition;
        this.ctx = ctx;
        this.operations = operations;
        this.applied = appliedSink;
    }

    @Override
    protected void preStart() {
        long stored = operations.loadStoredOffset(ctx.topic(), ctx.partition());
        initToken(stored);
        if (stored < 0) {
            ctx.seekToBeginning();
        } else {
            ctx.seek(stored + 1);
        }
    }

    @Override
    public PersistenceRunner.PersistenceEvent<RewindMessage> apply(ConsumedRecord<String> record) {
        applied.add(record.partition() + ":" + record.offset() + ":" + record.value());
        RewindMessage message = new RewindMessage(record.partition(), record.offset(), record.value());
        return PersistenceRunner.newEvent(message);
    }

    @Override
    public PersistenceRunner.SideEffectProcessor process() {
        return sideEffect -> { /* no post-commit side effect needed */ };
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void onError(ConsumedRecord<String> record, Exception e) {
    }
}
