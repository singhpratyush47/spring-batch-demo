package com.processing.batch.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;

/**
 * Exposes a REST endpoint to trigger the batch job on demand.
 *
 * POST /api/jobs/hello-world
 *
 * In production you'd typically trigger via:
 *   - A scheduler (e.g., Spring's @Scheduled, Quartz, or an external cron)
 *   - A message queue event
 *   - This REST endpoint (for on-demand / manual runs)
 */
@RestController
@RequestMapping("/api/jobs")
public class JobController {

    private static final Logger log = LoggerFactory.getLogger(JobController.class);

    private final JobLauncher jobLauncher;
    private final Job helloWorldJob;
    private final Job streamedJob;

    public JobController(JobLauncher jobLauncher, Job helloWorldJob, Job streamedJob) {
        this.jobLauncher = jobLauncher;
        this.helloWorldJob = helloWorldJob;
        this.streamedJob=streamedJob;
    }

    @PostMapping("/hello-world")
    public ResponseEntity<String> runJob() throws Exception {
        // run.id makes each request a unique JobInstance so the job can re-run
        JobParameters params = new JobParametersBuilder()
                .addLong("run.id", System.currentTimeMillis())
                .toJobParameters();

        var execution = jobLauncher.run(helloWorldJob, params);

        String response = "JobExecutionId=%d | Status=%s"
                .formatted(execution.getId(), execution.getStatus());

        log.info("Triggered via REST → {}", response);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/run")
    public ResponseEntity<String> run(
            @RequestParam(defaultValue = "2024-01-15") String date) throws Exception {

        JobParameters params = new JobParametersBuilder()
                .addLocalDate("batch.date", LocalDate.parse(date))   // identifying → part of JOB_KEY
                .toJobParameters();

        log.info("Launching job with batch.date={}", date);
        var execution = jobLauncher.run(streamedJob, params);

        String response = "JobExecutionId=%d | Status=%s | ExitCode=%s"
                .formatted(execution.getId(), execution.getStatus(),
                        execution.getExitStatus().getExitCode());

        log.info("Job result → {}", response);
        return ResponseEntity.ok(response);
    }

    /**
     * Force-rerun endpoint — for re-running a COMPLETED job with the same date.
     *
     * Adds a non-identifying run.id (false = non-identifying) so the JOB_KEY
     * is different, creating a fresh JobInstance while keeping batch.date meaningful.
     * Use this for manual reruns, reprocessing scenarios, or ops-triggered reruns.
     */
    @PostMapping("/force-rerun")
    public ResponseEntity<String> forceRerun(
            @RequestParam(defaultValue = "2024-01-15") String date) throws Exception {

        JobParameters params = new JobParametersBuilder()
                .addLocalDate("batch.date", LocalDate.parse(date))          // identifying
                .addLong("run.id", System.currentTimeMillis(), false)        // NON-identifying
                .toJobParameters();

        log.info("Force-rerunning job with batch.date={} (new JobInstance)", date);
        var execution = jobLauncher.run(streamedJob, params);

        String response = "JobExecutionId=%d | Status=%s | ExitCode=%s"
                .formatted(execution.getId(), execution.getStatus(),
                        execution.getExitStatus().getExitCode());

        log.info("Job result → {}", response);
        return ResponseEntity.ok(response);
    }
}
