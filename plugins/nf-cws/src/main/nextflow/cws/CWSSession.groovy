package nextflow.cws

import nextflow.Session
import nextflow.cws.script.CWSProcessFactory
import nextflow.script.BaseScript
import nextflow.script.ProcessFactory

/**
 * Central, global instance to manage all generated Executor instances
 */
class CWSSession extends Session {

    static final CWSSession INSTANCE = new CWSSession()

    private Set<SchedulerClient> schedulerClients = new HashSet<>()

    synchronized addSchedulerClient( SchedulerClient schedulerClient ) {
        schedulerClients.add(schedulerClient)
    }

    synchronized Set<SchedulerClient> getSchedulerClients() {
        new HashSet(schedulerClients)
    }

    private CWSSession() {}

    @Override
    ProcessFactory newProcessFactory(BaseScript script) {
        new CWSProcessFactory(script, this)
    }

}
