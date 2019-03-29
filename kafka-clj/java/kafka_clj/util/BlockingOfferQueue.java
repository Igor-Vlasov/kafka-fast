package kafka_clj.util;

import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;

public class BlockingOfferQueue<E> extends ArrayBlockingQueue<E> {
    public BlockingOfferQueue(int capacity) {
        super(capacity);
    }

    public BlockingOfferQueue(int capacity, boolean fair) {
        super(capacity, fair);
    }

    public BlockingOfferQueue(int capacity, boolean fair, Collection<? extends E> c) {
        super(capacity, fair, c);
    }

    @Override
    public boolean offer(E e) {
        try {
            super.put(e);
            return true;
        } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
