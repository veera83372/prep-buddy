package org.apache.prepbuddy.exceptions;

public class ErrorMessage {
    private final String key;
    private final String message;

    public ErrorMessage(String key, String message) {
        this.key = key;
        this.message = message;
    }
}
