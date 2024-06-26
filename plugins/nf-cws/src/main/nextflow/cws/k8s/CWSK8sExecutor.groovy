package nextflow.cws.k8s

import com.google.common.hash.Hashing
import groovy.transform.CompileStatic
import groovy.transform.Memoized
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.cws.CWSConfig
import nextflow.cws.CWSSchedulerBatch
import nextflow.cws.SchedulerClient
import nextflow.cws.k8s.client.CWSK8sClient
import nextflow.cws.k8s.model.CWSPodOptions
import nextflow.cws.k8s.model.PodMountConfigWithMode
import nextflow.cws.processor.CWSTaskPollingMonitor
import nextflow.k8s.K8sConfig
import nextflow.k8s.K8sExecutor
import nextflow.k8s.client.K8sClient
import nextflow.k8s.client.K8sResponseException
import nextflow.k8s.model.PodHostMount
import nextflow.k8s.model.PodVolumeClaim
import nextflow.processor.TaskHandler
import nextflow.processor.TaskMonitor
import nextflow.processor.TaskRun
import nextflow.util.Duration
import nextflow.util.ServiceName
import org.pf4j.ExtensionPoint

import java.nio.file.Paths

@Slf4j
@CompileStatic
@ServiceName('k8s')
class CWSK8sExecutor extends K8sExecutor implements ExtensionPoint {

    @PackageScope SchedulerClient schedulerClient
    @PackageScope CWSSchedulerBatch schedulerBatch

    private CWSK8sClient client
    /**
     * Name of the created daemonSet
     */
    private String daemonSet = null

    @Override
    @Memoized
    protected K8sConfig getK8sConfig() {
        return new CWSK8sConfig( (Map<String,Object>)session.config.k8s )
    }

    /**
     * @return A {@link nextflow.processor.TaskMonitor} associated to this executor type
     */
    @Override
    protected TaskMonitor createTaskMonitor() {
        CWSConfig cwsConfig = new CWSConfig(session.config.navigate('cws') as Map)
        if ( cwsConfig.getBatchSize() > 1 ) {
            this.schedulerBatch = new CWSSchedulerBatch( cwsConfig.getBatchSize() )
        }
        return CWSTaskPollingMonitor.create( session, name, 100, Duration.of('5 sec'), this.schedulerBatch )
    }

    /**
     * Creates a {@link nextflow.processor.TaskHandler} for the given {@link nextflow.processor.TaskRun} instance
     *
     * @param task A {@link nextflow.processor.TaskRun} instance representing a process task to be executed
     * @return A {@link nextflow.k8s.K8sTaskHandler} instance modeling the execution in the K8s cluster
     */
    @Override
    TaskHandler createTaskHandler(TaskRun task) {
        assert task
        assert task.workDir
        log.trace "[K8s] launching process > ${task.name} -- work folder: ${task.workDirStr}"
        return new CWSK8sTaskHandler( task, this )
    }

    @Override
    protected K8sClient getClient() {
        return client
    }

    @Override
    protected void register() {
        super.register()

        final k8sConfig = getK8sConfig()
        final clientConfig = k8sConfig.getClient()
        this.client = new CWSK8sClient(clientConfig)
        log.debug "[K8s] config=$k8sConfig; API client config=$clientConfig"

        final CWSK8sConfig cwsK8sConfig = k8sConfig as CWSK8sConfig

        if ( cwsK8sConfig.locationAwareScheduling() ) {
            createDaemonSet()
            registerGetStatsConfigMap()
        }

        CWSK8sConfig.K8sScheduler k8sSchedulerConfig = (k8sConfig as CWSK8sConfig).getScheduler()
        final CWSConfig cwsConfig = new CWSConfig(session.config.navigate('cws') as Map)
        Map data

        if ( !k8sSchedulerConfig && !cwsConfig.dns ) {
            //Use default configuration
            k8sSchedulerConfig = CWSK8sConfig.K8sScheduler.defaultConfig( k8sConfig )
        }

        if( k8sSchedulerConfig ) {
            final CWSPodOptions podOptions = cwsK8sConfig.getPodOptions()
            schedulerClient = new K8sSchedulerClient(
                    cwsConfig,
                    k8sSchedulerConfig,
                    cwsK8sConfig,
                    cwsK8sConfig.getNamespace(),
                    session.runName,
                    client,
                    podOptions.getVolumeClaims(),
                    podOptions.getHostMounts()
            )
            Boolean traceEnabled = session.config.navigate('trace.enabled') as Boolean
            CWSK8sConfig.Storage storage = cwsK8sConfig.getStorage()
            data = [
                    volumeClaims : podOptions.getVolumeClaims(),
                    traceEnabled : traceEnabled,
                    costFunction : cwsConfig.getCostFunction(),
                    memoryPredictor : cwsConfig.getMemoryPredictor(),
                    additional   : k8sSchedulerConfig.getAdditional(),
                    workDir : storage?.getWorkdir(),
                    copyStrategy : storage?.getCopyStrategy(),
                    locationAware : cwsK8sConfig.locationAwareScheduling(),
                    maxCopyTasksPerNode : cwsConfig.getMaxCopyTasksPerNode(),
                    maxWaitingCopyTasksPerNode : cwsConfig.getMaxWaitingCopyTasksPerNode()
            ]
        } else {
            data = [
                    dns : cwsConfig.dns,
                    namespace : k8sConfig.getNamespace(),
                    costFunction : cwsConfig.getCostFunction(),
            ]
            schedulerClient = new SchedulerClient( cwsConfig, session.runName )
        }
        this.schedulerBatch?.setSchedulerClient( schedulerClient )
        schedulerClient.registerScheduler( data )
    }

    @Override
    void shutdown() {
        final CWSK8sConfig.K8sScheduler schedulerConfig = (k8sConfig as CWSK8sConfig).getScheduler()
        if( schedulerConfig ) {
            try{
                schedulerClient.closeScheduler()
            } catch (Exception e){
                log.error( "Error while closing scheduler", e)
            }
        }

        if( daemonSet ){
            try {
                def result = client.daemonSetDelete( daemonSet )
                log.trace "$result"
            } catch (K8sResponseException e){
                log.error("Couldn't delete daemonset: $daemonSet", e)
            }
        }
        log.trace "Close K8s Executor"
    }

    protected void registerGetStatsConfigMap() {
        Map<String,String> configMap = [:]

        String architectureStatFileName
        final String architecture = System.getProperty("os.arch");
        if (architecture == "amd64" || architecture == "x86_64") {
            architectureStatFileName = "getStatsAndResolveSymlinks_linux_x86"
        } else if (architecture == "aarch64" || architecture == "arm64") {
            architectureStatFileName = "getStatsAndResolveSymlinks_linux_aarch64"
        } else {
            throw new RuntimeException("The ${architecture} architecture is by default not supported for WOW." +
                    "You may compile the getStatsAndResolveSymlinks.c yourself and add it to the resources directory.")
        }
        final contentStream = CWSK8sExecutor.class.getResourceAsStream(architectureStatFileName)
        final content = contentStream.bytes.encodeBase64().toString()
        configMap[WOWK8sWrapperBuilder.statFileName] = content

        String configMapName = makeConfigMapName(content)
        tryCreateConfigMap(configMapName, configMap)
        log.debug "Created K8s configMap with name: $configMapName"
        k8sConfig.getPodOptions().getMountConfigMaps().add( new PodMountConfigWithMode(configMapName, '/etc/nextflow', 0111) )
    }

    protected void tryCreateConfigMap(String name, Map<String,String> data) {
        try {
            client.configCreateBinary(name, data)
        }
        catch( K8sResponseException e ) {
            if( e.response.reason != 'AlreadyExists' )
                throw e
        }
    }

    protected static String makeConfigMapName(String content ) {
        "nf-get-stat-${hash(content)}"
    }

    protected static String hash(String text) {
        def hasher = Hashing .murmur3_32() .newHasher()
        hasher.putUnencodedChars(text)
        return hasher.hash().toString()
    }

    private void createDaemonSet(){

        final K8sConfig k8sConfig = getK8sConfig()
        final CWSPodOptions podOptions = (k8sConfig as CWSK8sConfig).getPodOptions()
        final mounts = []
        final volumes = []
        int volume = 1

        // host mounts
        for( PodHostMount entry : podOptions.hostMounts ) {
            final name = 'vol-' + volume++
            mounts << [name: name, mountPath: entry.mountPath]
            volumes << [name: name, hostPath: [path: entry.hostPath]]
        }

        final namesMap = [:]

        // creates a volume name for each unique claim name
        for( String claimName : podOptions.volumeClaims.collect { it.claimName }.unique() ) {
            final volName = 'vol-' + volume++
            namesMap[claimName] = volName
            volumes << [name: volName, persistentVolumeClaim: [claimName: claimName]]
        }

        // -- volume claims
        for( PodVolumeClaim entry : podOptions.volumeClaims ) {
            //check if we already have a volume for the pvc
            final name = namesMap.get(entry.claimName)
            final claim = [name: name, mountPath: entry.mountPath ]
            if( entry.subPath )
                claim.subPath = entry.subPath
            if( entry.readOnly )
                claim.readOnly = entry.readOnly
            mounts << claim
        }

        String name = "mount-${session.runName.replace('_', '-')}"
        def spec = [
                containers: [ [
                                      name: name,
                                      image: (k8sConfig as CWSK8sConfig).getStorage().getImageName(),
                                      volumeMounts: mounts,
                                      imagePullPolicy : 'IfNotPresent'
                              ] ],
                volumes: volumes,
                serviceAccount: client.config.serviceAccount
        ]

        CWSK8sConfig.Storage storage = (k8sConfig as CWSK8sConfig).getStorage()
        if( storage.getNodeSelector() )
            spec.put( 'nodeSelector', storage.getNodeSelector().toSpec() as Serializable )

        def pod = [
                apiVersion: 'apps/v1',
                kind: 'DaemonSet',
                metadata: [
                        labels: [
                                app: 'nextflow'
                        ],
                        name: name,
                        namespace: k8sConfig.getNamespace() ?: 'default'
                ],
                spec : [
                        restartPolicy: 'Always',
                        template: [
                                metadata: [
                                        labels: [
                                                name : name,
                                                app: 'nextflow'
                                        ]
                                ],
                                spec: spec,
                        ],
                        selector: [
                                matchLabels: [
                                        name: name
                                ]
                        ]
                ]
        ]

        daemonSet = name
        client.daemonSetCreate(pod, Paths.get('.nextflow-daemonset.yaml') )
        log.trace "Created daemonSet: $name"

    }

}
