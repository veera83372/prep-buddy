package org.apache.prepbuddy.smoothingops;

import org.apache.prepbuddy.exceptions.ApplicationException;
import org.apache.prepbuddy.exceptions.ErrorMessages;

public class WeightedSlidingWindow extends SlidingWindow {
    private Weights weights;

    public WeightedSlidingWindow(int size, Weights weights) {
        super(size);
        if (weights.size() != size)
            throw new ApplicationException(ErrorMessages.WINDOW_SIZE_AND_WEIGHTS_SIZE_NOT_MATCHING);
        this.weights = weights;
    }

    @Override
    public void add(double value) {
        if (isFull()) {
            queue.remove();
        }
        int size = queue.size();
        Double weightValue = weights.get(size) * value;
        queue.add(weightValue);
    }

    public Double average() {
        return sum();
    }
}
