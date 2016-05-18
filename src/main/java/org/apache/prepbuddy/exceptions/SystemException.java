package org.apache.prepbuddy.exceptions;

public class SystemException extends RuntimeException {

    private Exception e;

    public SystemException(Exception e) {
        this.e = e;
    }

    @Override
    public String getMessage() {
        return e.getMessage();
    }
}
