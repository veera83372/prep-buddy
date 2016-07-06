package org.apache.datacommons.prepbuddy.smoothers

import org.apache.datacommons.prepbuddy.SparkTestCase
import org.junit.Assert._

class SimpleSlidingWindowTest extends SparkTestCase{
    test("should add value till window is full than move values to left") {
        val slidingWindow: SimpleSlidingWindow = new SimpleSlidingWindow(3)
        slidingWindow.add(3)
        slidingWindow.add(3)
        slidingWindow.add(3)

        assertEquals(3.0, slidingWindow.average, 0.01)

        slidingWindow.add(9)
        assertEquals(5.0, slidingWindow.average, 0.01)

        slidingWindow.add(15)
        assertEquals(9.0, slidingWindow.average, 0.01)
    }
}
