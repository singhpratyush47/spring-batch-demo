package com.processing.batch;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Full integration test — boots the entire Spring context and runs the real job
 * against the in-memory H2 database.
 *
 * @SpringBatchTest injects JobLauncherTestUtils and JobRepositoryTestUtils automatically.
 */
@SpringBatchTest
@SpringBootTest
@ActiveProfiles("test")
class HelloWorldJobIntegrationTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private Job helloWorldJob;

    @Test
    void job_shouldComplete_withStatusCompleted() throws Exception {
        jobLauncherTestUtils.setJob(helloWorldJob);

        JobParameters params = new JobParametersBuilder()
                .addLong("run.id", System.currentTimeMillis())
                .toJobParameters();

        JobExecution execution = jobLauncherTestUtils.launchJob(params);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(execution.getStepExecutions()).hasSize(1);

        var stepExecution = execution.getStepExecutions().iterator().next();
        assertThat(stepExecution.getReadCount()).isEqualTo(9);
        assertThat(stepExecution.getWriteCount()).isEqualTo(9);
        assertThat(stepExecution.getSkipCount()).isZero();
    }

    @Test
    void step_shouldProcessAllItems_inChunksOf3() throws Exception {
        jobLauncherTestUtils.setJob(helloWorldJob);

        JobExecution execution = jobLauncherTestUtils.launchStep("helloWorldStep");

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        var stepExecution = execution.getStepExecutions().iterator().next();
        // 9 items / chunk-size 3 = 3 commits
        assertThat(stepExecution.getCommitCount()).isEqualTo(4); // 3 chunk commits + 1 final
    }
}
