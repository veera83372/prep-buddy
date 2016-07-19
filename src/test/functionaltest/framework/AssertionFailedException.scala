package framework

class AssertionFailedException(errorMessage: String) extends Throwable {
    override def getMessage: String = errorMessage
}
