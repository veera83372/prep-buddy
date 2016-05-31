package org.apache.prepbuddy.utils;

public class ProductOfNumbers {

    private double accumulator;

    public ProductOfNumbers(double seed) {
        this.accumulator = seed;
    }

    public ProductOfNumbers() {
        this(1.0);
    }

    public void multiply(double number) {
        accumulator = accumulator * number;
    }

    public double result() {
        return accumulator;
    }
}
