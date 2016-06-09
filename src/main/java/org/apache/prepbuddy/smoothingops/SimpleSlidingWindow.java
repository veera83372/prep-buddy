package org.apache.prepbuddy.smoothingops;

public class SimpleSlidingWindow extends SlidingWindow {

    public SimpleSlidingWindow(int size) {
        super(size);
    }

    public Double average() {
        return Double.valueOf(sum() / size);
    }

}
