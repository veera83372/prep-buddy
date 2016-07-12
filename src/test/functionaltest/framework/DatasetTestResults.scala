package framework

class DatasetTestResults(testSpecName: String) {
    private var result: String = null
    private var cause: AssertionError = null

    def markAsSuccess() {
        result = "SUCCESSFUL"
    }

    def markAsFailure(error: AssertionError) {
        result = "FAILURE"
        cause = error
    }

    def getOutcome: String = result

    def getCause: String = cause.getMessage
}
