package org.apache.prepbuddy.smoothingops;

public class WeightedSlidingWindow extends SlidingWindow {
    private Weights weights;

    public WeightedSlidingWindow(int size, Weights weights) {
        super(size);
        this.weights = weights;
    }

    @Override
    public void add(double value) {
        if (isFull())
            queue.remove();
        int size = queue.size();
        Double weightValue = weights.get(size);
        queue.add(weightValue);
    }

    public Double average() {
        return sum();
    }
}
