package org.apache.prepbuddy.datasmoothers;

import java.util.LinkedList;
import java.util.Queue;

public class SimpleSlidingWindow {

    private int size;
    private Queue<Double> queue;

    public SimpleSlidingWindow(int size) {
        queue = new LinkedList<>();
        this.size = size;
    }

    public void add(double value) {
        if (isFull())
            queue.remove();
        queue.add(value);
    }

    public boolean isFull() {
        return queue.size() == size;
    }

    public int sum() {
        int sum = 0;
        for (Double oneValue : queue) {
            sum += oneValue;
        }
        return sum;
    }

    public Double average() {
        return Double.valueOf(sum() / size);
    }

}
