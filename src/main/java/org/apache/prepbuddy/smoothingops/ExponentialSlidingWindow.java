package org.apache.prepbuddy.smoothingops;

public class ExponentialSlidingWindow extends SlidingWindow {
    private final double inverseWeightFactor;
    private final double weightFactor;
    private Double previousAverage = null;

    public ExponentialSlidingWindow(double weightFactor) {
        super(2);
        this.weightFactor = weightFactor;
        this.inverseWeightFactor = 1 - weightFactor;
    }

    @Override
    public Double average() {
        if (previousAverage == null)
            previousAverage = queue.peek();
        previousAverage = (queue.getLast() * weightFactor) + (previousAverage * inverseWeightFactor);
        return previousAverage;
    }
}
