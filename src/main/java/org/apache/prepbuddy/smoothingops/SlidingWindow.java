package org.apache.prepbuddy.smoothingops;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.Queue;

public abstract class SlidingWindow implements Serializable {
    protected int size;
    protected Queue<Double> queue;

    public SlidingWindow(int size) {
        this.size = size;
        queue = new LinkedList<>();
    }

    public void add(double value) {
        if (isFull())
            queue.remove();
        queue.add(value);
    }

    public boolean isFull() {
        return queue.size() == size;
    }

    public Double sum() {
        Double sum = 0.0;
        for (Double oneValue : queue) {
            sum += oneValue;
        }
        return sum;
    }

    public abstract Double average();
}
