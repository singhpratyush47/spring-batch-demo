package com.processing.batch.model;

import java.time.LocalDate;

/**
 * Represents a single unit of work flowing through the batch pipeline.
 * In a real job this would be your domain object (Order, Transaction, etc.)
 */
public record Greeting(
        int id,
        String message,
        String format,
        LocalDate date) {

    public Greeting(int id, String message) {
        this(id, message, null, null);
    }
}
