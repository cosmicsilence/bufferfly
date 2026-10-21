package io.bufferfly.core.actor;

/**
 * Thrown by an {@link ActorDispatcher} when an actor's {@link Mailbox} refuses
 * to accept a message — i.e. the bounded mailbox was full and the offer timed
 * out before a slot became available.
 *
 * <p>This is a backpressure signal distinct from
 * {@link io.bufferfly.core.backpressure.UnderPressureException}: that one is
 * raised <em>proactively</em> by the backpressure layer under the {@code THROW}
 * strategy, whereas this one is raised <em>reactively</em> when the mailbox
 * itself is saturated. Upstream producers (e.g. a Kafka consumer) should treat
 * both the same way: stop producing, let the system drain, and redeliver.
 */
public class MailBoxRefusedException extends RuntimeException {

    public MailBoxRefusedException(String message) {
        super(message);
    }
}
