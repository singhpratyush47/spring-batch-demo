package com.processing.batch.config;

import com.processing.batch.listener.JobCompletionListener;
import com.processing.batch.model.Greeting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.List;
import java.util.stream.IntStream;

/**
 * Wires together a single-step Spring Batch job:
 *
 *   READ (ListItemReader)  →  PROCESS (uppercase)  →  WRITE (log output)
 *
 * Chunk size = 3: Spring Batch reads/processes 3 items, then commits in one transaction.
 * This is the fundamental knob for throughput vs. memory trade-off in production.
 */
@Configuration
public class HelloWorldJobConfig {

    private static final Logger log = LoggerFactory.getLogger(HelloWorldJobConfig.class);

    private static final int CHUNK_SIZE = 3;

    // ─── Reader ───────────────────────────────────────────────────────────────

    /**
     * In production this is typically a FlatFileItemReader, JdbcCursorItemReader,
     * or a JpaPagingItemReader. Using an in-memory list here for simplicity.
     */
    @Bean
    public ItemReader<Greeting> greetingReader() {
        List<Greeting> items = IntStream.rangeClosed(1, 9)
                .mapToObj(i -> new Greeting(i, "hello world #" + i))
                .toList();
        return new ListItemReader<>(items);
    }

    // ─── Processor ────────────────────────────────────────────────────────────

    /**
     * Transforms each item. Returning null drops the item (filtered out of the write phase).
     * In production: validation, enrichment, format conversion, etc.
     */
    @Bean
    public ItemProcessor<Greeting, Greeting> greetingProcessor() {
        return item -> {
            Greeting processed = new Greeting(item.id(), item.message().toUpperCase());
            log.debug("  processed → {}", processed);
            return processed;
        };
    }

    // ─── Writer ───────────────────────────────────────────────────────────────

    /**
     * Receives a chunk (List) of processed items in one call per commit interval.
     * In production: JdbcBatchItemWriter, FlatFileItemWriter, KafkaItemWriter, etc.
     */
    @Bean
    public ItemWriter<Greeting> greetingWriter() {
        return chunk -> chunk.getItems()
                .forEach(g -> log.info("  ✔ written → [{} | {}]", g.id(), g.message()));
    }

    // ─── Step ─────────────────────────────────────────────────────────────────

    @Bean
    public Step helloWorldStep(JobRepository jobRepository,
                               PlatformTransactionManager transactionManager,
                               ItemReader<Greeting> greetingReader,
                               ItemProcessor<Greeting, Greeting> greetingProcessor,
                               ItemWriter<Greeting> greetingWriter) {
        return new StepBuilder("helloWorldStep", jobRepository)
                .<Greeting, Greeting>chunk(CHUNK_SIZE, transactionManager)
                .reader(greetingReader)
                .processor(greetingProcessor)
                .writer(greetingWriter)
                /*
                 * Production hooks — uncomment as needed:
                 *
                 * .faultTolerant()
                 *     .skipLimit(10)
                 *     .skip(ValidationException.class)
                 *     .retryLimit(3)
                 *     .retry(TransientDataAccessException.class)
                 * .listener(new StepExecutionListener() { ... })
                 */
                .build();
    }

    // ─── Job ──────────────────────────────────────────────────────────────────

    @Bean
    public Job helloWorldJob(JobRepository jobRepository,
                             Step helloWorldStep,
                             JobCompletionListener listener) {
        return new JobBuilder("helloWorldJob", jobRepository)
                .listener(listener)
                .start(helloWorldStep)
                /*
                 * Multi-step example (uncomment when you add a second step):
                 *
                 * .next(anotherStep)
                 */
                .build();
    }
}
