package io.bufferfly.core.persistence;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An actor implementation of {@link AbstractPersistentActor} that receives {@link Tweet}
 * messages and delegates their persistence to a {@link PersistenceRunner}.
 */
public class TweeterActor extends AbstractPersistentActor<Tweet, Tweet> {

    private final String actorName;
    private final AtomicInteger receiveCount = new AtomicInteger(0);
    private final List<Tweet> received = Collections.synchronizedList(new ArrayList<>());
    private final List<Tweet> errorTweets = Collections.synchronizedList(new ArrayList<>());
    private final List<Exception> errorExceptions = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger counterSideEffects = new AtomicInteger();

    public TweeterActor(String actorName, PersistenceConfig<Tweet> config) {
        super(config);
        if (actorName == null) throw new NullPointerException("actorName must not be null");
        this.actorName = actorName;
    }

    public TweeterActor(PersistenceConfig<Tweet> config) {
        this("tweeter-actor", config);
    }

    public TweeterActor(String actorName, PersistenceRunner<Tweet> runner) {
        super(runner);
        if (actorName == null) throw new NullPointerException("actorName must not be null");
        this.actorName = actorName;
    }

    public TweeterActor(PersistenceRunner<Tweet> runner) {
        this("tweeter-actor", runner);
    }

    @Override
    public PersistenceRunner.SideEffectProcessor process() {
        return sideEffect -> counterSideEffects.incrementAndGet();
    }

    @Override
    public PersistenceRunner.PersistenceEvent<Tweet> apply(Tweet message) {
        receiveCount.incrementAndGet();
        received.add(message);
        return PersistenceRunner.newEvent(message, new TweetSideEffect(message));
    }

    public int countSideEffects() {
        return counterSideEffects.get();
    }

    record TweetSideEffect(Tweet tweet) implements PersistenceRunner.SideEffect {
    }

    @Override
    public String name() {
        return actorName;
    }

    @Override
    public void onError(Tweet message, Exception e) {
        errorTweets.add(message);
        errorExceptions.add(e);
    }

    public int getReceiveCount() {
        return receiveCount.get();
    }

    public List<Tweet> getReceived() {
        return Collections.unmodifiableList(received);
    }

    public List<Tweet> getErrorTweets() {
        return Collections.unmodifiableList(errorTweets);
    }

    public List<Exception> getErrorExceptions() {
        return Collections.unmodifiableList(errorExceptions);
    }
}
