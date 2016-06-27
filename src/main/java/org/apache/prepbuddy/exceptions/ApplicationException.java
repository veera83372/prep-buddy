package org.apache.prepbuddy.exceptions;

public class ApplicationException extends RuntimeException {

    private ErrorMessage message;

    public ApplicationException(ErrorMessage message) {
        this.message = message;
    }

    @Override
    public String getMessage() {
        return message.getMessage();
    }
}
