package org.apache.prepbuddy.smoothers;

public class SimpleSlidingWindow extends SlidingWindow {

    public SimpleSlidingWindow(int size) {
        super(size);
    }

    public Double average() {
        return Double.valueOf(sum() / size);
    }

}
