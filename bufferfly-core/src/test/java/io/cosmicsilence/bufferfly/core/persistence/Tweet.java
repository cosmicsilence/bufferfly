package io.cosmicsilence.bufferfly.core.persistence;

/**
 * Represents a Tweet with an identifier and text content.
 *
 * @param id   unique identifier of the tweet (maps to ID column)
 * @param text content of the tweet (maps to TEXT VARCHAR2(100) column)
 */
public record Tweet(String id, String text) {

    public Tweet {
        if (id == null) {
            throw new NullPointerException("id must not be null");
        }
        if (text == null) {
            throw new NullPointerException("text must not be null");
        }
    }
}
