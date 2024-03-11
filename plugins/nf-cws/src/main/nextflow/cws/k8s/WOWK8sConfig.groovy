package nextflow.cws.k8s


import groovy.transform.CompileStatic;
import groovy.transform.PackageScope
import nextflow.cws.processor.WOWTaskRun
import nextflow.exception.AbortOperationException
import nextflow.k8s.client.K8sClient
import nextflow.k8s.model.PodHostMount;
import nextflow.k8s.model.PodNodeSelector;
import nextflow.k8s.model.PodOptions;

import java.nio.file.Path

public class WOWK8sConfig extends CWSK8sConfig {

    private Map<String, Object> target

    private PodOptions podOptions

    WOWK8sConfig(Map<String, Object> config) {
        super(config)
        this.target = config

        if (getLocalPath()) {
            final name = getLocalPath()
            final mount = getLocalStorageMountPath()
            this.podOptions.hostMount.add(new PodHostMount(name, mount))
        }
    }

    @Override
    K8sScheduler getScheduler() {
        locationAwareScheduling() ? new K8sScheduler((Map<String, Object>) target.scheduler) : super.getScheduler()
    }

    Storage getStorage() {
        locationAwareScheduling() ? new Storage((Map<String, Object>) target.storage, getLocalClaimPaths()) : null
    }

    String getLocalPath() {
        target.localPath as String
    }

    String getLocalStorageMountPath() {
        target.localStorageMountPath ?: '/workspace' as String
    }

    boolean locationAwareScheduling() {
        getLocalClaimPaths().size() > 0
    }

    Collection<String> getLocalClaimPaths() {
        podOptions.hostMount.collect { it.mountPath }
    }

    String findLocalVolumeClaimByPath(String path) {
        def result = podOptions.hostMount.find { path.startsWith(it.mountPath) }
        return result ? result.hostPath : null
    }

    void checkStorageAndPaths(K8sClient client, String pipelineName) {
        super.checkStorageAndPaths(client)
        //The nextflow project/workflow has to be on a shared drive
        if (pipelineName && pipelineName[0] == '/' && !findVolumeClaimByPath(pipelineName))
            throw new AbortOperationException("Kubernetes `pipelineName` must be a path mounted as a persistent volume -- projectDir=$pipelineName; volumes=${getClaimPaths().join(', ')}")

        if (getStorage() && !findLocalVolumeClaimByPath(getStorage().getWorkdir()))
            throw new AbortOperationException("Kubernetes `storage.workdir` must be a path mounted as a local volume -- storage.workdir=${getStorage().getWorkdir()}; volumes=${getLocalClaimPaths().join(', ')}")
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
            target.imageName ?: 'fondahub/vsftpd:latest'
        }

        String getCmd() {
            target.cmd as String ?: "./$WOWTaskRun.CMD_INIT_RUN"
        }

        boolean withInitContainers() {
            return "true".equalsIgnoreCase(target.initContainers as String)
        }

        /**
         * If copy process is not running together with the main pod. If this is set, initContainers will be ignored
         * @return
         */
        boolean separateCopy() {
            return true
        }

    }
}
