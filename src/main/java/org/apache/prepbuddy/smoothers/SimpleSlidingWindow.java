package org.apache.prepbuddy.smoothers;

/**
 * A sliding window which calculates the unweighted mean of a window
 * for Simple Moving Average.
 */
public class SimpleSlidingWindow extends SlidingWindow {

    public SimpleSlidingWindow(int size) {
        super(size);
    }

    public Double average() {
        return Double.valueOf(sum() / size);
    }

}
