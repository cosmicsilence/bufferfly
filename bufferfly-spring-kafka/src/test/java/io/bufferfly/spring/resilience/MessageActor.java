package io.bufferfly.spring.resilience;

import io.bufferfly.core.persistence.AbstractPersistentActor;
import io.bufferfly.core.persistence.PersistenceConfig;
import io.bufferfly.core.persistence.PersistenceRunner;
import io.bufferfly.spring.ConsumedRecord;
import io.bufferfly.spring.KafkaContext;

/**
 * A persistent, per-partition actor that stores each consumed string and stamps
 * it with a per-partition auto-incremented {@code id}.
 *
 * <h2>Id auto-increment (and why it is actor-owned)</h2>
 * The next id is a plain {@code long} advanced on the single actor thread — no
 * synchronization needed (single-writer). It is <b>rehydrated from the database</b>
 * on {@link #preStart()} ({@code maxId(partition) + 1}), so a brand-new actor
 * instance — e.g. one spun up on a <em>different consumer</em> after a rebalance
 * moved this partition — continues the sequence from the last durably stored id
 * instead of restarting at 1. Combined with the offset rewind below, that is what
 * makes a failover lose no messages and create no duplicates.
 *
 * <h2>Offset rewind</h2>
 * On {@link #preStart()} the actor seeks to the next offset after the one durably
 * stored in the DB (the fencing token), so it resumes exactly where the previous
 * owner left off.
 */
public final class MessageActor extends AbstractPersistentActor<ConsumedRecord<String>, Message> {

    private final String name;
    private final int partition;
    private final KafkaContext ctx;
    private final MessageOperations operations;

    /** Next per-partition id to assign. Actor-thread only; rehydrated on preStart. */
    private long nextId = 1L;

    public MessageActor(String name,
                        int partition,
                        KafkaContext ctx,
                        MessageOperations operations,
                        PersistenceConfig<Message> config) {
        super(config);
        this.name = name;
        this.partition = partition;
        this.ctx = ctx;
        this.operations = operations;
    }

    @Override
    protected void preStart() {
        // Rehydrate the id sequence from the authoritative DB state so a fresh
        // instance (after restart or failover to another consumer) continues the
        // per-partition sequence rather than colliding or restarting at 1.
        nextId = operations.maxId(partition) + 1;

        // Resume from the last durably committed offset (fencing token source).
        long stored = operations.loadStoredOffset(ctx.topic(), ctx.partition());
        if (stored < 0) {
            ctx.seekToBeginning();
        } else {
            ctx.seek(stored + 1);
        }
    }

    @Override
    public PersistenceRunner.PersistenceEvent<Message> apply(ConsumedRecord<String> record) {
        long id = nextId++;
        Message message = new Message(id, record.partition(), record.offset(), record.value());
        return PersistenceRunner.newEvent(message);
    }

    @Override
    public PersistenceRunner.SideEffectProcessor process() {
        return sideEffect -> { /* no post-commit side effect needed for the suite */ };
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void onError(ConsumedRecord<String> record, Exception e) {
        // minimal for the suite
    }
}
