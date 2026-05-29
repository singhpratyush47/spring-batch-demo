package com.processing.batch.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;

/**
 * Hooks into the Job lifecycle — ideal place for:
 *  - sending notifications (Slack, email) on success/failure
 *  - recording audit events
 *  - triggering downstream jobs
 */
@Component
public class JobCompletionListener implements JobExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(JobCompletionListener.class);

    @Override
    public void beforeJob(JobExecution jobExecution) {
        log.info("▶  Job '{}' starting  | runId={}",
                jobExecution.getJobInstance().getJobName(),
                jobExecution.getJobParameters().getLong("run.id", -1L));
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        log.info("■  Job '{}' finished  | status={}  | duration={}ms",
                jobExecution.getJobInstance().getJobName(),
                jobExecution.getStatus(),
                jobExecution.getEndTime() != null
                        ? java.time.Duration.between(jobExecution.getStartTime(), jobExecution.getEndTime()).toMillis()
                        : -1);
    }
}
