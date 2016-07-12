package org.apache.datacommons.prepbuddy.smoothers

import org.scalatest.FunSuite

class SimpleSlidingWindowTest extends FunSuite {
    test("should add value till window is full than move values to left") {
        val slidingWindow: SimpleSlidingWindow = new SimpleSlidingWindow(3)
        slidingWindow.add(3)
        slidingWindow.add(3)
        slidingWindow.add(3)

        assert(3.0 == slidingWindow.average, 0.01)

        slidingWindow.add(9)
        assert(5.0 == slidingWindow.average, 0.01)

        slidingWindow.add(15)
        assert(9.0 == slidingWindow.average, 0.01)
    }
}
