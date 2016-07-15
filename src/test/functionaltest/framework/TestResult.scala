package framework

class TestResult(testName: String) {
    private var message: String = null
    private var cause: AssertionError = null
    private var passed: Boolean = false

    def markAsSuccess() {
        message = "PASSED => ".concat(testName)
        passed = true
    }

    def markAsFailure(error: AssertionError) {
        cause = error
        message = "FAILED => " + testName + "\r\n" +
            getCause + "\n" +
            error.getStackTrace.mkString("\r\n")
        passed = false
    }

    def getOutcome: String = message

    def getCause: String = cause.getMessage

    def isFailed: Boolean = !passed
}
