package nextflow.cws.processor

import nextflow.processor.TaskRun

public class WOWTaskRun extends TaskRun {
    /**
     * The number of times the initialization of the task has failed
     */
    volatile int initFailCount

    /**
     * Mark task as using init
     */
    volatile boolean withInit = false

    /**
     * Mark task as initialized; default true, as most environments do not initialize tasks
     */
    volatile boolean initialized = true

    @Override
    TaskRun makeCopy() {
        TaskRun copy = super.makeCopy()
        if (withInit) copy.initialized = false
        if (initialized) copy.initFailCount = 0 // <-- start counting anew if it was initialized once successfully.    }
        return copy
    }

    static final public String CMD_TRACE_SCHEDULER = '.command.scheduler.trace'
    static final public String CMD_INIT_LOG = '.command.init.log'
    static final public String CMD_INIT_RUN = '.command.init.run'
}
