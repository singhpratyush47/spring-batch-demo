package com.processing.batch.listener;

import com.processing.batch.model.Greeting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.SkipListener;
import org.springframework.stereotype.Component;

@Component
public class CustomerSkipListener implements SkipListener<Greeting, Greeting> {

    private static final Logger log = LoggerFactory.getLogger(CustomerSkipListener.class);

    // In production inject a repository to write skipped items to a dead-letter table
    // private final DeadLetterRepository deadLetterRepo;

    /**
     * Called when an item is skipped during READ.
     * Note: you don't get the item here — the read failed so there's no item object.
     * Only the exception and whatever context you can extract from it.
     */
    @Override
    public void onSkipInRead(Throwable t) {
        log.error("SKIP in READ — reason: {}", t.getMessage(), t);
        // In production: alert ops, increment a metric, write to audit log
    }

    /**
     * Called when an item is skipped during PROCESS.
     * You have the full item — persist it to a dead-letter table with the reason.
     */
    @Override
    public void onSkipInProcess(Greeting item, Throwable t) {
        log.error("SKIP in PROCESS — item: {} | reason: {}", item, t.getMessage(), t);
        // deadLetterRepo.save(new DeadLetterRecord(item.id(), t.getMessage(), "PROCESS"));
    }

    /**
     * Called when an item is skipped during WRITE.
     * Item made it through processing but failed at write time.
     */
    @Override
    public void onSkipInWrite(Greeting item, Throwable t) {
        log.error("SKIP in WRITE — item: {} | reason: {}", item, t.getMessage(), t);
        // deadLetterRepo.save(new DeadLetterRecord(item.id(), t.getMessage(), "WRITE"));
    }
}