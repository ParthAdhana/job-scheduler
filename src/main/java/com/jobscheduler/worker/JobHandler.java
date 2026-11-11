package com.jobscheduler.worker;

/**
 * The work behind one job type. Add a new job type by adding a Spring bean that implements this.
 *
 * <p>Handlers must be safe to run more than once for the same job: the scheduler guarantees a job
 * is not run twice concurrently, but a worker can crash after doing the work and before recording
 * success, and then the job is retried.
 */
public interface JobHandler {

    /** The value clients pass as {@code type} when submitting a job. */
    String type();

    /** Throwing marks the attempt as failed. */
    void execute(JobContext context) throws Exception;
}
