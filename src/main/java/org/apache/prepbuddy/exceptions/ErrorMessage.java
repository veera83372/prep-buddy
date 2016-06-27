package org.apache.prepbuddy.exceptions;

import java.io.Serializable;

public class ErrorMessage implements Serializable {
    private final String key;
    private final String message;

    public ErrorMessage(String key, String message) {
        this.key = key;
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}
