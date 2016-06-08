package org.apache.prepbuddy.utils;

import java.util.LinkedList;
import java.util.Queue;

public class SlidingWindow {
    private int size;
    private Queue<Double> queue;

    public SlidingWindow(int size) {
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

    public Double average() {
        return Double.valueOf(sum() / size);
    }

    public int sum() {
        int sum = 0;
        for (Double oneValue : queue) {
            sum += oneValue;
        }
        return sum;
    }

}
