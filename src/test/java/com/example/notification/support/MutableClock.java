package com.example.notification.support;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

/**
 * A {@link Clock} whose instant only moves when {@link #advance(Duration)} is called.
 * Lets tests drive the rate limiter's token refill deterministically and quickly.
 */
public class MutableClock extends Clock {

    private Instant instant;
    private final ZoneId zone;

    public MutableClock(Instant instant, ZoneId zone) {
        this.instant = instant;
        this.zone = zone;
    }

    public void advance(Duration amount) {
        this.instant = this.instant.plus(amount);
    }

    @Override
    public Instant instant() {
        return instant;
    }

    @Override
    public long millis() {
        return instant.toEpochMilli();
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return new MutableClock(instant, zone);
    }
}
