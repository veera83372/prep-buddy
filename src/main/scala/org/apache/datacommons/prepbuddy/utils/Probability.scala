package org.apache.datacommons.prepbuddy.utils

class Probability(probabilityValue: Double) extends Serializable {
    def divide(other: Probability): Probability = {
        new Probability(this.probabilityValue / other.doubleValue)
    }

    def multiply(other: Probability): Probability = {
        new Probability(this.probabilityValue * other.doubleValue)
    }

    def doubleValue: Double = probabilityValue
}
