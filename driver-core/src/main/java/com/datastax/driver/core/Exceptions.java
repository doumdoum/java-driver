package com.datastax.driver.core;

import java.util.concurrent.ExecutionException;

import com.datastax.driver.core.exceptions.DriverException;

class Exceptions {
    /** Helps dealing with execution exception when waiting on a future. */
    static RuntimeException toUnchecked(ExecutionException e) {
        if (e.getCause() instanceof Error) {
            throw ((Error)e.getCause());
        } else if (e.getCause() instanceof RuntimeException) {
            return ((RuntimeException)e.getCause());
        } else {
            return new DriverException("Unexpected error while initializing session", e.getCause());
        }
    }
}
