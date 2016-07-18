package framework

class DuplicateTestNameException(errorMessage: String) extends Throwable {
    override def getMessage: String = errorMessage
}
