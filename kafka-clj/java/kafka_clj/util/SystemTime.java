package kafka_clj.util;

import org.apache.kafka.common.utils.Time;

class SystemTime implements Time {
    public long milliseconds() {
        return System.currentTimeMillis();
    }

    public long nanoseconds() {
        return System.nanoTime();
    }

    public long hiResClockMs() {
        return 0;
    }

    public void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            // Ignore
        }
    }
}