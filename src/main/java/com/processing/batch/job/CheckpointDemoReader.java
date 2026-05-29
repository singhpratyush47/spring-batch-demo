package com.processing.batch.job;

import com.processing.batch.model.Greeting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;

import java.util.List;

/**
 * A stateful ItemStreamReader that persists its read position into the
 * ExecutionContext after every chunk commit.
 *
 * WHY THIS MATTERS FOR RESTART:
 *   ListItemReader implements ItemReader, NOT ItemStreamReader.
 *   That means Spring Batch has no way to serialize its position — it
 *   cannot save a checkpoint. On restart the reader would start from 0
 *   and reprocess everything.
 *
 *   ItemStreamReader adds three lifecycle methods:
 *     open()   — called once at step start; restores position from DB on restart
 *     update() — called after every chunk commit; saves current position to DB
 *     close()  — called when step ends; release resources (file handles, cursors etc.)
 *
 *   Spring Batch writes the ExecutionContext to BATCH_STEP_EXECUTION_CONTEXT
 *   after every update() call. On restart it calls open() with the last
 *   saved context, so the reader fast-forwards to the right position.
 *
 * IN PRODUCTION:
 *   You'd use JdbcCursorItemReader, FlatFileItemReader, JpaPagingItemReader —
 *   all of which already implement ItemStreamReader correctly.
 *   This class exists purely to make the checkpoint mechanism visible to you.
 */
public class CheckpointDemoReader implements ItemStreamReader<Greeting> {

    private static final Logger log = LoggerFactory.getLogger(CheckpointDemoReader.class);

    // Key used to store/retrieve position in BATCH_STEP_EXECUTION_CONTEXT
    private static final String POSITION_KEY = "checkpoint.demo.reader.position";

    private final List<Greeting> items;
    private int currentPosition = 0;

    public CheckpointDemoReader(List<Greeting> items) {
        this.items = items;
    }

    /**
     * Called once when the step starts.
     * On a FRESH run: executionContext is empty, start from 0.
     * On a RESTART:  executionContext has the last committed position from DB,
     *                fast-forward the reader to skip already-processed items.
     */
    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        if (executionContext.containsKey(POSITION_KEY)) {
            currentPosition = executionContext.getInt(POSITION_KEY);
            log.info("♻  RESTART detected — resuming from position {} (items 1–{} already processed)",
                    currentPosition, currentPosition);
        } else {
            log.info("▶  FRESH run — starting from position 0");
        }
    }

    /**
     * Called after every chunk commit by Spring Batch.
     * Saves current position into the ExecutionContext, which Batch then
     * flushes to BATCH_STEP_EXECUTION_CONTEXT in the DB.
     * This is the checkpoint.
     */
    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        executionContext.putInt(POSITION_KEY, currentPosition);
        log.debug("  checkpoint saved at position {}", currentPosition);
    }

    @Override
    public void close() throws ItemStreamException {
        log.debug("  reader closed at position {}", currentPosition);
    }

    /**
     * Returns null when exhausted (signals end-of-data to Spring Batch).
     */
    @Override
    public Greeting read() {
        if (currentPosition >= items.size()) {
            return null;
        }
        Greeting item = items.get(currentPosition++);
        log.debug("  read → {}", item);
        return item;
    }
}
