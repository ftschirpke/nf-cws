package nextflow.cws.script

import nextflow.Session
import nextflow.cws.processor.WOWTaskProcessor
import nextflow.executor.Executor
import nextflow.processor.TaskProcessor
import nextflow.script.BaseScript
import nextflow.script.BodyDef
import nextflow.script.ProcessConfig
import nextflow.script.ProcessFactory

class CWSProcessFactory extends ProcessFactory {
    private BaseScript owner
    private Session session

    CWSProcessFactory(BaseScript ownerScript, Session session ) {
        super(ownerScript, session)
        this.owner = ownerScript
        this.session = session
    }

    @Override
    protected TaskProcessor newTaskProcessor(String name, Executor executor, ProcessConfig config, BodyDef taskBody ) {
        new WOWTaskProcessor(name, executor, session, owner, config, taskBody)
    }
}
