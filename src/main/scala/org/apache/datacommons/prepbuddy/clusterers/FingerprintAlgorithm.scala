package org.apache.datacommons.prepbuddy.clusterers

import java.util.regex.Pattern

abstract class FingerprintAlgorithm extends ClusteringAlgorithm {
    private val PunctuationMatcher: Pattern = Pattern.compile("\\p{Punct}|[\\x00-\\x08\\x0A-\\x1F\\x7F]")

    def removeAllPunctuations(someString: String): String = PunctuationMatcher.matcher(someString).replaceAll("")
}
