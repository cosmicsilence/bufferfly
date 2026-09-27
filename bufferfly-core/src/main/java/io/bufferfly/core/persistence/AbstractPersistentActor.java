package io.bufferfly.core.persistence;

import io.bufferfly.core.actor.AbstractActor;

/**
 * Base class for actors that need to persist the result of processing each
 * incoming message.
 *
 * <h2>Processing model</h2>
 * <pre>
 *   Kafka message ──► receive(T)
 *                          │
 *                          ▼
 *                      apply(T)          ← user implements: pure transformation,
 *                          │               in-memory rules, state decisions
 *                          ▼
 *                    PersistenceRunner
 *                      .enqueue(E)       ← non-blocking hand-off to I/O layer
 * </pre>
 *
 * <p>The separation keeps the actor loop CPU-bound: no blocking I/O, no network
 * calls, no transaction management. The {@link PersistenceRunner} owns the I/O
 * thread and batches events for efficient bulk writes via
 * {@link TransactionalOperations}.
 *
 * <h2>Implementing</h2>
 * <pre>{@code
 * public class PaymentActor extends AbstractPersistentActor<KafkaEvent, PaymentRecord> {
 *
 *     public PaymentActor(PersistenceRunner<PaymentRecord> runner) {
 *         super(runner);
 *     }
 *
 *     \@Override public String name() { return "payment-actor"; }
 *
 *     \@Override
 *     public PaymentRecord apply(KafkaEvent event) {
 *         return new PaymentRecord(event.id(), event.amount());
 *     }
 *
 *     \@Override
 *     public void onError(KafkaEvent event, Exception e) { /* log or metric *\/ }
 * }
 * }</pre>
 *
 * @param <T> the incoming message type (e.g. a Kafka record value)
 * @param <E> the persistence event type produced by {@link #apply}
 */
public abstract class AbstractPersistentActor<T, E> extends AbstractActor<T> {

    private final PersistenceRunner<E> runner;

    protected AbstractPersistentActor(PersistenceRunner<E> runner) {
        if (runner == null) throw new NullPointerException("runner must not be null");
        this.runner = runner;
    }

    /**
     * Transforms an incoming message into a persistence event.
     *
     * <p>Must be pure and non-blocking. All I/O, batching, and transaction
     * management are handled by the {@link PersistenceRunner}.
     *
     * @param message the message received by this actor
     * @return the persistence event to enqueue; must not be {@code null}
     */
    public abstract E apply(T message);

    /**
     * Calls {@link #apply} then enqueues the result with the runner.
     * Do not override — override {@link #apply} instead.
     */
    @Override
    public final void receive(T message) {
        runner.enqueue(apply(message));
    }
}
