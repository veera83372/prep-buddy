package org.apache.prepbuddy.datasmoothers;

import java.util.LinkedList;
import java.util.Queue;

public class WeightedSlidingWindow {
    private int size;
    private Queue<Double> queue;

    public WeightedSlidingWindow(int size) {
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
        return weightedSum() / sumOfWeights();
    }

    private int sumOfWeights() {
        int sumOfWeights = 0;
        for (int i = 1; i <= size; i++)
            sumOfWeights += i;
        return sumOfWeights;
    }

    private double weightedSum() {
        int weight = 1;
        int sum = 0;
        for (Double oneValue : queue) {
            sum += oneValue * weight;
            weight++;
        }
        return sum;
    }
}
