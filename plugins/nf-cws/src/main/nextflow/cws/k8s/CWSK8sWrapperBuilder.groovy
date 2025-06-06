package nextflow.cws.k8s

import groovy.transform.CompileStatic
import nextflow.k8s.K8sWrapperBuilder
import nextflow.processor.TaskRun

@CompileStatic
class CWSK8sWrapperBuilder extends K8sWrapperBuilder {

    CWSK8sWrapperBuilder(TaskRun task, boolean memoryPredictorEnabled) {
        super(task)
        if (memoryPredictorEnabled) {
            //Always collect stats for memory prediction
            this.statsEnabled = true
        }
    }

}
