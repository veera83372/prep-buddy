package org.apache.datacommons.prepbuddy.smoothers

import org.scalatest.FunSuite

class WeightedSlidingWindowTest extends FunSuite {
    test("should add value till window is full then move values to left in every add") {
        val weights: Weights = new Weights(3)
        weights.add(0.2)
        weights.add(0.3)
        weights.add(0.5)

        val slidingWindow: WeightedSlidingWindow = new WeightedSlidingWindow(3, weights)
        slidingWindow.add(3)
        slidingWindow.add(4)
        slidingWindow.add(5)

        val expected: Double = 4.3
        val actual: Double = slidingWindow.average
        assert(expected == actual)

        slidingWindow.add(3)
        val expected1: Double = 3.8
        val actual1: Double = slidingWindow.average
        assert(expected1 == actual1)
    }
}
