package nextflow.cws

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.dag.DAG
import nextflow.trace.TraceObserver
import nextflow.trace.TraceRecord
import nextflow.processor.TaskHandler

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
        CWSSession.INSTANCE.getSchedulerClients().each { schedulerClient ->
            schedulerClient.submitVertices( dag.vertices )
            schedulerClient.submitEdges( dag.edges )
        }
    }

    @Override
    void onProcessComplete(TaskHandler handler, TraceRecord trace) {
        CWSSession.INSTANCE.getSchedulerClients().each { schedulerClient ->
            Map schedulerRequirements = schedulerClient.getDataRequirements()
            if ( schedulerRequirements == null ) {
                return
            }
            for ( String key : schedulerRequirements.keySet() ) {
                if ( key != "traceFields" ) {
                    throw new GroovyRuntimeException("Got unsupported scheduler requirement: ${key}")
                }
            }
            def traceFields = schedulerRequirements.traceFields
            if ( traceFields ) {
                Map traceData = traceFields.collect{ entry -> trace.get(entry) }
                schedulerClient.submitTraceData( handler.getTask().id, traceData )
            }
        }
    }

}
