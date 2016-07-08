package org.apache.datacommons.prepbuddy.exceptions

class ApplicationException(msg: String) extends Throwable{
    override def getMessage: String = msg
}
