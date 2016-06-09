package org.apache.prepbuddy.datasmoothers;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.Queue;

public class WeightedSlidingWindow implements Serializable {
    private int size;
    private Queue<Double> queue;
    private Weights weights;

    public WeightedSlidingWindow(int size, Weights weights) {
        this.weights = weights;
        queue = new LinkedList<>();
        this.size = size;
    }

    public void add(double value) {
        if (isFull())
            queue.remove();
        int index = queue.size();
        queue.add(value * weights.get(index));
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

    public Double average() {
        return sum();
    }
}
