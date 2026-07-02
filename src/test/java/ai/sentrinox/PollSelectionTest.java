package ai.sentrinox;

import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link SccalReferenceSync#isOneShot}: the poll_interval value
 * that decides between a single capture and the polling loop. Pure logic — no
 * DB, no network.
 */
class PollSelectionTest {

    @Test
    void zeroIntervalIsOneShot() {
        assertTrue(SccalReferenceSync.isOneShot(Duration.ZERO));
    }

    @Test
    void negativeIntervalIsOneShot() {
        assertTrue(SccalReferenceSync.isOneShot(Duration.ofSeconds(-5)));
    }

    @Test
    void positiveIntervalPolls() {
        assertFalse(SccalReferenceSync.isOneShot(Duration.ofSeconds(30)));
        assertFalse(SccalReferenceSync.isOneShot(Duration.ofMillis(1)));
    }

    @Test
    void defaultConfigValuePolls() {
        // The shipped default (poll_interval = 1m) keeps the poll loop alive.
        Duration shipped = ConfigFactory.load().getConfig("analytics_cdc").getDuration("poll_interval");
        assertFalse(SccalReferenceSync.isOneShot(shipped),
            "default poll_interval should keep polling");
    }

    @Test
    void hoconDurationStringPolls() {
        // A configured "5 minutes" parses to a positive duration → polling.
        Duration configured = ConfigFactory.parseString("d = 5 minutes").getDuration("d");
        assertFalse(SccalReferenceSync.isOneShot(configured));
    }
}
