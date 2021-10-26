package org.query.calc;

import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.ints.IntHeapPriorityQueue;

public final class SlidingWindowPriorityQueue {
    private final int windowSize;
    private final IntComparator comparator;
    private final IntHeapPriorityQueue queue;

    public SlidingWindowPriorityQueue(int windowSize, IntComparator comparator) {
        this.windowSize = windowSize;
        this.comparator = comparator;
        queue = new IntHeapPriorityQueue(windowSize, comparator);
    }

    public int size() {
        return queue.size();
    }

    public void enqueue(int i) {
        if (queue.size() < windowSize) {
            queue.enqueue(i);
        } else if (comparator.compare(i, queue.firstInt()) > 0) {
            queue.dequeueInt();
            queue.enqueue(i);
        }
    }

    public int dequeue() {
        return queue.dequeueInt();
    }
}
