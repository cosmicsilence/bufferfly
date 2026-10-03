package io.cosmicsilence.bufferfly.core.actor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link BlockingMailbox}.
 *
 * Covers: basic offer/poll, ordering, size/clear, capacity behaviour,
 * and concurrent race-condition scenarios using virtual threads.
 * All asynchronous assertions use Awaitility.
 */
@Timeout(30)
class BlockingMailboxTest {

    private BlockingMailbox<String> mailbox;

    @BeforeEach
    void setUp() {
        mailbox = new BlockingMailbox<>(1_000);
    }

    // -----------------------------------------------------------------------
    // Basic synchronous behaviour
    // -----------------------------------------------------------------------

    @Test
    void should_returnSameMessage_when_singleMessageOfferedAndPolled() {
        assertTrue(mailbox.offer("hello"));
        assertEquals("hello", mailbox.poll());
    }

    @Test
    void should_preserveFifoOrder_when_multipleMessagesOffered() {
        mailbox.offer("first");
        mailbox.offer("second");
        mailbox.offer("third");

        assertEquals("first",  mailbox.poll());
        assertEquals("second", mailbox.poll());
        assertEquals("third",  mailbox.poll());
    }

    @Test
    void should_reflectCurrentDepth_when_messagesAreOfferedAndPolled() {
        assertEquals(0, mailbox.size());
        mailbox.offer("a");
        mailbox.offer("b");
        assertEquals(2, mailbox.size());
        mailbox.poll();
        assertEquals(1, mailbox.size());
    }

    @Test
    void should_reportZeroSize_when_clearCalledAfterOffers() {
        mailbox.offer("a");
        mailbox.offer("b");
        mailbox.clear();
        assertEquals(0, mailbox.size());
    }

    // -----------------------------------------------------------------------
    // Capacity behaviour
    // -----------------------------------------------------------------------

    @Test
    void should_acceptNewOffer_when_slotFreedAfterCapacityReached() {
        BlockingMailbox<String> tiny = new BlockingMailbox<>(2);
        assertTrue(tiny.offer("1"));
        assertTrue(tiny.offer("2"));
        tiny.poll(); // free a slot
        assertTrue(tiny.offer("3"));
        assertEquals(2, tiny.size());
    }

    // -----------------------------------------------------------------------
    // Concurrent race conditions
    // -----------------------------------------------------------------------

    @Test
    void should_enqueueAllMessages_when_manyConcurrentProducersOffer() throws InterruptedException {
        final int THREADS    = 20;
        final int PER_THREAD = 50;
        final int TOTAL      = THREADS * PER_THREAD;

        BlockingMailbox<Integer> mb = new BlockingMailbox<>(TOTAL + 1);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch done      = new CountDownLatch(THREADS);

        for (int t = 0; t < THREADS; t++) {
            final int base = t * PER_THREAD;
            Thread.ofVirtual().start(() -> {
                try { startGate.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                for (int i = 0; i < PER_THREAD; i++) mb.offer(base + i);
                done.countDown();
            });
        }

        startGate.countDown();
        done.await(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> assertEquals(TOTAL, mb.size(),
                   "All produced messages must reside in the mailbox"));
    }

    @Test
    void should_loseNoMessages_when_singleProducerAndConsumerRunConcurrently() {
        final int MESSAGES = 500;
        BlockingMailbox<Integer> mb = new BlockingMailbox<>(MESSAGES);
        AtomicInteger consumed = new AtomicInteger(0);

        Thread.ofVirtual().start(() -> {
            for (int i = 0; i < MESSAGES; i++) mb.offer(i);
        });

        Thread.ofVirtual().start(() -> {
            int seen = 0;
            while (seen < MESSAGES) {
                Integer v = mb.poll();
                if (v != null) seen++;
            }
            consumed.set(seen);
        });

        await().atMost(Duration.ofSeconds(15))
               .untilAsserted(() -> assertEquals(MESSAGES, consumed.get(),
                   "Consumer must see every produced message"));
    }

    @Test
    void should_accountForAllMessages_when_manyProducersAndConsumersRunConcurrently() throws InterruptedException {
        final int PRODUCER_COUNT = 10;
        final int CONSUMER_COUNT = 10;
        final int PER_PRODUCER   = 100;
        final int TOTAL          = PRODUCER_COUNT * PER_PRODUCER;

        BlockingMailbox<Integer> mb = new BlockingMailbox<>(TOTAL + 1);
        AtomicInteger consumed   = new AtomicInteger(0);
        CountDownLatch startGate = new CountDownLatch(1);

        for (int p = 0; p < PRODUCER_COUNT; p++) {
            final int base = p * PER_PRODUCER;
            Thread.ofVirtual().start(() -> {
                try { startGate.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                for (int i = 0; i < PER_PRODUCER; i++) mb.offer(base + i);
            });
        }

        int perConsumer = TOTAL / CONSUMER_COUNT;
        for (int c = 0; c < CONSUMER_COUNT; c++) {
            Thread.ofVirtual().start(() -> {
                try { startGate.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                for (int i = 0; i < perConsumer; i++) {
                    Integer v = mb.poll();
                    if (v != null) consumed.incrementAndGet();
                }
            });
        }

        startGate.countDown();

        await().atMost(Duration.ofSeconds(20))
               .untilAsserted(() -> assertEquals(TOTAL, consumed.get() + mb.size(),
                   "Total produced must equal consumed + queued"));
    }

    @Test
    void should_notThrow_when_clearCalledWhileConcurrentProducersAreOffering() throws InterruptedException {
        BlockingMailbox<Integer> mb = new BlockingMailbox<>(10_000);
        CountDownLatch startGate    = new CountDownLatch(1);
        AtomicInteger producersDone = new AtomicInteger(0);

        for (int t = 0; t < 5; t++) {
            Thread.ofVirtual().start(() -> {
                try { startGate.await(); } catch (InterruptedException ignored) {}
                for (int i = 0; i < 200; i++) mb.offer(i);
                producersDone.incrementAndGet();
            });
        }

        startGate.countDown();
        for (int i = 0; i < 5; i++) {
            assertDoesNotThrow(mb::clear);
            Thread.sleep(1);
        }

        await().atMost(Duration.ofSeconds(10))
               .untilAsserted(() -> assertEquals(5, producersDone.get(),
                   "All producer threads must finish"));
    }
}
