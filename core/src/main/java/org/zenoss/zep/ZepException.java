/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep;

/**
 * Represents a general exception which can occur by Zep while processing
 * events.
 */
public class ZepException extends Exception {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new exception with no message and no cause information.
     */
    public ZepException() {
        super();
    }

    /**
     * Creates a new exception with the specified message and cause.
     * 
     * @param message
     *            The exception message.
     * @param cause
     *            The underlying cause of the exception.
     */
    public ZepException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new exception with the specified message.
     * 
     * @param message
     *            The exception message.
     */
    public ZepException(String message) {
        super(message);
    }

    /**
     * Creates a new exception with the specified cause.
     * 
     * @param cause
     *            The underlying cause of the exception.
     */
    public ZepException(Throwable cause) {
        super(cause);
    }

}
