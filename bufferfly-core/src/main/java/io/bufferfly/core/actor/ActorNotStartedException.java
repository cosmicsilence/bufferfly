package io.bufferfly.core.actor;

public class ActorNotStartedException extends RuntimeException {

    public ActorNotStartedException(String message) {
        super(message);
    }
}
