package org.apache.datacommons.prepbuddy.qualityanalyzers

abstract class DataType {
    def matchingCount(sampleData: List[String]): Int

    protected def matches(regex: String, samples: List[String]): Int = {
        val matches: List[String] = samples.filter(_.matches(regex))
        matches.size
    }
}

object ALPHANUMERIC_STRING extends DataType {
    override def matchingCount(sampleData: List[String]): Int = sampleData.size
}

object DECIMAL extends DataType {
    override def matchingCount(sampleData: List[String]): Int = {
        val EXPRESSION: String = "^[+-]?(\\.\\d+|\\d+\\.\\d+)$"
        matches(EXPRESSION, sampleData)
    }
}

object INTEGER extends DataType {
    override def matchingCount(sampleData: List[String]): Int = {
        val EXPRESSION: String = "^[+-]?\\d+$"
        matches(EXPRESSION, sampleData)
    }
}

object EMAIL extends DataType {
    override def matchingCount(sampleData: List[String]): Int = {
        val EXPRESSION: String = "^[_A-Za-z0-9-]+(\\.[_A-Za-z0-9-]+)*@[A-Za-z0-9]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$"
        matches(EXPRESSION, sampleData)
    }
}

object CURRENCY extends DataType {
    override def matchingCount(sampleData: List[String]): Int = {
        val EXPRESSION: String = "^(\\p{Sc})(\\d+|\\d+.\\d+)$"
        matches(EXPRESSION, sampleData)
    }
}

object URL extends DataType {
    override def matchingCount(sampleData: List[String]): Int = {
        val EXPRESSION: String = "^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]$"
        matches(EXPRESSION, sampleData)
    }
}

object IP_ADDRESS extends DataType {
    override def matchingCount(sampleData: List[String]): Int = {
        val EXPRESSION: String = "^(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])$"
        matches(EXPRESSION, sampleData)
    }
}

object ZIP_CODE_US extends DataType {
    override def matchingCount(sampleData: List[String]): Int = {
        val EXPRESSION: String = "^[0-9]{5}(?:-[0-9]{4})?$"
        matches(EXPRESSION, sampleData)
    }
}

object MOBILE_NUMBER extends DataType {
    override def matchingCount(sampleData: List[String]): Int = {
        val EXPRESSION: String = "^(([+]\\d+\\s)|0)?\\d{10}$"
        matches(EXPRESSION, sampleData)
    }
}

object SOCIAL_SECURITY_NUMBER extends DataType {
    override def matchingCount(sampleData: List[String]): Int = {
        val EXPRESSION: String = "^(\\d{3}-\\d{2}-\\d{4})$"
        matches(EXPRESSION, sampleData)
    }
}

object TIMESTAMP extends DataType {
    override def matchingCount(sampleData: List[String]): Int = {
        val EXPRESSION: String = "(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}[Z]?)"
        matches(EXPRESSION, sampleData)
    }
}

object LONGITUDE extends DataType {
    override def matchingCount(sampleData: List[String]): Int = {
        val EXPRESSION: String = "^[-+](180(\\.0+)?|((1[0-7]\\d)|([1-9]?\\d))(\\.\\d+)?)$"
        matches(EXPRESSION, sampleData)
    }
}

object LATITUDE extends DataType {
    override def matchingCount(sampleData: List[String]): Int = {
        val EXPRESSION: String = "^[-+]([1-8]?\\d(\\.\\d+)?|90(\\.0+)?)$"
        matches(EXPRESSION, sampleData)
    }
}

