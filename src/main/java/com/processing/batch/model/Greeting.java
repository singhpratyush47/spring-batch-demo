package com.processing.batch.model;

/**
 * Represents a single unit of work flowing through the batch pipeline.
 * In a real job this would be your domain object (Order, Transaction, etc.)
 */
public record Greeting(int id, String message) {}
