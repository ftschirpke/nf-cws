package nextflow.cws.k8s

import groovy.transform.CompileStatic
import groovy.transform.Memoized
import groovy.transform.PackageScope
import nextflow.exception.AbortOperationException
import nextflow.k8s.K8sConfig
import nextflow.k8s.client.K8sClient
import nextflow.k8s.model.PodHostMount
import nextflow.k8s.model.PodNodeSelector

import java.nio.file.Path
import java.util.stream.Collectors

@CompileStatic
class CWSK8sConfig extends K8sConfig {

    private Map<String,Object> target
    final boolean schedulingIsLocationAware

    CWSK8sConfig(Map<String, Object> config, boolean isLocationAware) {
        super(config)
        this.target = config
        this.schedulingIsLocationAware = isLocationAware
        if ( getLocalPath() ) {
            final name = getLocalPath()
            final mount = getLocalStorageMountPath()
            getPodOptions().mountHostPaths.add(new PodHostMount(name, mount))
        }
    }

    K8sScheduler getScheduler(){
        target.scheduler || schedulingIsLocationAware ? new K8sScheduler( (Map<String,Object>)target.scheduler ) : null
    }

    Storage getStorage() {
        schedulingIsLocationAware ? new Storage((Map<String, Object>) target.storage, getLocalClaimPaths()) : null
    }

    String getArchitecture() {
        target.architecture ?: System.getProperty("os.arch")
    }

    String getLocalPath() {
        target.localPath as String
    }

    String getLocalStorageMountPath() {
        target.localStorageMountPath ?: '/workspace' as String
    }

    Collection<String> getLocalClaimPaths() {
        getPodOptions().mountHostPaths.collect { it.mountPath }
    }

    String findLocalVolumeClaimByPath(String path) {
        def result = getPodOptions().mountHostPaths.find { path.startsWith(it.mountPath) }
        return result ? result.hostPath : null
    }

    void checkStorageAndPaths(K8sClient client, String pipelineName) {
        super.checkStorageAndPaths(client)
        if ( schedulingIsLocationAware ) {
            //The nextflow project/workflow has to be on a shared drive
            if (pipelineName && pipelineName[0] == '/' && !findVolumeClaimByPath(pipelineName))
                throw new AbortOperationException("Kubernetes `pipelineName` must be a path mounted as a persistent volume -- projectDir=$pipelineName; volumes=${getClaimPaths().join(', ')}")

            if (getStorage() && !findLocalVolumeClaimByPath(getStorage().getWorkdir()))
                throw new AbortOperationException("Kubernetes `storage.workdir` must be a path mounted as a local volume -- storage.workdir=${getStorage().getWorkdir()}; volumes=${getLocalClaimPaths().join(', ')}")
        }
    }

    @CompileStatic
    @PackageScope
    static class K8sScheduler {

        Map<String,Object> target

        private final String[] fields = [
                'name',
                'serviceAccount',
                'cpu',
                'memory',
                'container',
                'command',
                'port',
                'workDir',
                'runAsUser',
                'autoClose',
                'nodeSelector',
                'imagePullPolicy'
        ]

        K8sScheduler(Map<String,Object> scheduler) {
            this.target = scheduler
        }

        String getName() { target.name as String ?: 'workflow-scheduler' }

        String getServiceAccount() { target.serviceAccount as String }

        // If no container is specified pull the latest image
        String getImagePullPolicy() { target.container ? target.imagePullPolicy as String : "Always" }

        Integer getCPUs() { target.cpu as Integer ?: 1 }

        String getMemory() { target.memory as String ?: "1400Mi" }

        String getContainer() { target.container as String ?: 'commonworkflowscheduler/kubernetesscheduler:v2.1' }

        String getCommand() { target.command as String }

        Integer getPort() { target.port as Integer ?: 8080 }

        String getWorkDir() { target.workDir as String }

        Integer runAsUser() { target.runAsUser as Integer }

        Boolean autoClose() { target.autoClose == null ? true : target.autoClose as Boolean }

        PodNodeSelector getNodeSelector(){
            return target.nodeSelector ? new PodNodeSelector( target.nodeSelector ) : null
        }

        Map<String,Object> getAdditional() {
            return target.entrySet()
                    .stream()
                    .filter{!(it.getKey() in fields) }
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
        }

        @Memoized
        static K8sScheduler defaultConfig( K8sConfig k8sConfig ){
            return new K8sScheduler([
                    "serviceAccount" : k8sConfig.getServiceAccount(),
                    "runAsUser" : 0,
                    "autoClose" : true
            ] as Map<String, Object>)
        }

    }

    @CompileStatic
    @PackageScope
    static class Storage {

        @Delegate
        Map<String,Object> target
        Collection<String> localClaims

        Storage(Map<String,Object> scheduler, Collection<String> localClaims ) {
            this.target = scheduler
            this.localClaims = localClaims
        }

        String getCopyStrategy() {
            target.copyStrategy as String ?: 'ftp'
        }

        String getWorkdir() {
            Path workdir = (target.workdir ?: localClaims[0]) as Path
            if( ! workdir.getName().equalsIgnoreCase('localWork') ){
                workdir = workdir.resolve( 'localWork' )
            }
            return workdir.toString()
        }

        PodNodeSelector getNodeSelector(){
            return target.nodeSelector ? new PodNodeSelector( target.nodeSelector ) : null
        }

        boolean deleteIntermediateData(){
            target.deleteIntermediateData as Boolean ?: false
        }

        String getImageName() {
            target.imageName ?: 'commonworkflowscheduler/ftpdaemon:v2.0'
        }

        String getCmd() {
            target.cmd as String
        }
    }
}