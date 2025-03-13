package nextflow.cws

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.dag.DAG
import nextflow.trace.TraceObserver

@Slf4j
@CompileStatic
class CWSObserver implements TraceObserver {

    private DAG dag

    @Override
    void onFlowCreate(Session session) {
        dag = session.dag
    }

    @Override
    void onFlowBegin() {
        dag.normalize()
        log.info("onFlowBegin")
        CWSSession.INSTANCE.getSchedulerClients().each { schedulerClient ->
            log.info("Submitting DAG:\n$dag.vertices\nand\n$dag.edges")
            schedulerClient.submitVertices( dag.vertices )
            schedulerClient.submitEdges( dag.edges )
        }
    }

}
