package com.processing.batch.config;

import com.processing.batch.job.CheckpointDemoReader;
import com.processing.batch.listener.CustomerSkipListener;
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
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.LocalDate;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Chunk size = 5, total items = 20.
 *
 * Checkpoint / restart demo:
 *   - Chunks 1 & 2 (items 1–10) commit successfully.
 *   - Item 13 (inside chunk 3) throws a RuntimeException.
 *   - Chunk 3 (items 11–15) is rolled back — those items are NOT written.
 *   - Job status → FAILED. BATCH_STEP_EXECUTION_CONTEXT holds position=10.
 *   - On restart with the same batch.date parameter, reader fast-forwards
 *     to position 10 and processes items 11–20.
 */
@Configuration
public class StreamedJobConfig {

    private static final Logger log = LoggerFactory.getLogger(StreamedJobConfig.class);

    private static final int CHUNK_SIZE = 5;
    private static final int TOTAL_ITEMS = 20;
    private static final int FAIL_ON_ITEM = 13;   // triggers failure mid-chunk-3

    // ─── Reader ───────────────────────────────────────────────────────────────

    /**
     * Using CheckpointDemoReader (implements ItemStreamReader) instead of
     * ListItemReader so Spring Batch can serialize the read position into
     * BATCH_STEP_EXECUTION_CONTEXT and restore it on restart.
     *
     * Scope is prototype — Spring Batch manages the lifecycle (open/update/close).
     */
    @Bean
    public ItemStreamReader<Greeting> streamedGreetingReader() {
        List<Greeting> items = IntStream.rangeClosed(1, TOTAL_ITEMS)
                .mapToObj(i -> new Greeting(i, "hello world #" + i))
                .toList();
        return new CheckpointDemoReader(items);
    }

    // ─── Processor ────────────────────────────────────────────────────────────

    /**
     * Simulates a real-world failure mid-chunk.
     * Item 13 blows up — chunk 3 (items 11–15) will be rolled back entirely.
     * Items 1–10 (chunks 1 & 2) are already committed and safe.
     */
    @Bean
    public ItemProcessor<Greeting, Greeting> streamedGreetingProcessor() {
        return item -> {
            if (item.id() == FAIL_ON_ITEM) {
                log.error("  ✗ simulated failure on item {} — throwing exception", item.id());
                throw new RuntimeException(
                        "Simulated processing failure on item " + item.id());
            }
            Greeting processed = new Greeting(item.id(), item.message().toUpperCase());
            log.info("  ✔ streamedGreetingProcessor → [{} | {}]", processed.id(), processed.message());
            return processed;
        };
    }

    @Bean
    public ItemProcessor<Greeting, Greeting> streamedGreetingEnrichedProcessor() {
        return item -> {
            Greeting processed = new Greeting(item.id(), item.message().toUpperCase(),"TEXT", LocalDate.now());
            log.info("  ✔ streamedGreetingEnrichedProcessor → [{} | {} | {} | {}]", processed.id(), processed.message(),processed.format(),processed.date());
            return processed;
        };
    }

    @Bean
    public CompositeItemProcessor<Greeting, Greeting> compositeProcessor(
            ItemProcessor<Greeting, Greeting> streamedGreetingProcessor,
            ItemProcessor<Greeting, Greeting> streamedGreetingEnrichedProcessor) {

        CompositeItemProcessor<Greeting, Greeting> processor =
                new CompositeItemProcessor<>();

        processor.setDelegates(List.of(
                streamedGreetingProcessor,
                streamedGreetingEnrichedProcessor
        ));

        return processor;
    }
    // ─── Writer ───────────────────────────────────────────────────────────────

    @Bean
    public ItemWriter<Greeting> streamedGreetingWriter() {
        return chunk -> {
            log.info("  ── writing chunk of {} items ──", chunk.getItems().size());
            chunk.getItems().forEach(g ->
                    log.info("     written → [{} | {}]", g.id(), g.message()));
        };
    }

    @Bean
    public FlatFileItemWriter<Greeting> greetingFileWriter() {

        log.info("  ── greetingFileWriter started writing ──");
        FlatFileItemWriter<Greeting> writer =
                new FlatFileItemWriter<>();

        writer.setName("greetingFileWriter");

        writer.setResource(
                new FileSystemResource("output/greetings.txt"));

        writer.setLineAggregator(greeting ->

                greeting.id() + "," +
                        greeting.message() + "," +
                        greeting.format() + "," +
                        greeting.date());
        log.info("  ── greetingFileWriter completed writing ──");
        return writer;
    }

    @Bean
    public CompositeItemWriter<Greeting> compositeWriter(
            ItemWriter<Greeting> streamedGreetingWriter,
            FlatFileItemWriter<Greeting> greetingFileWriter) {

        CompositeItemWriter<Greeting> writer =
                new CompositeItemWriter<>();

        writer.setDelegates(List.of(
                streamedGreetingWriter,
                greetingFileWriter
        ));

        return writer;
    }
    // ─── Step ─────────────────────────────────────────────────────────────────

    @Bean
    public Step streamedStep(JobRepository jobRepository,
                             PlatformTransactionManager transactionManager,
                             ItemStreamReader<Greeting> streamedGreetingReader,
                             CompositeItemProcessor<Greeting, Greeting> compositeProcessor,
                             CompositeItemWriter<Greeting> compositeWriter, CustomerSkipListener customerSkipListener) {
        return new StepBuilder("streamedStep", jobRepository)
                .<Greeting, Greeting>chunk(CHUNK_SIZE, transactionManager)
                .reader(streamedGreetingReader)
                .processor(compositeProcessor)
                .writer(compositeWriter)
                .faultTolerant()
                    .skipLimit(3)
                    .skip(RuntimeException.class)
                .listener(customerSkipListener)
                .build();
    }

    // ─── Job ──────────────────────────────────────────────────────────────────

    @Bean
    public Job streamedJob(JobRepository jobRepository,
                             Step streamedStep,
                             JobCompletionListener listener) {
        return new JobBuilder("streamedJob", jobRepository)
                .listener(listener)
                .start(streamedStep)
                .build();
    }
}
