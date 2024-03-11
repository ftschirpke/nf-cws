package nextflow.cws.k8s.model

import nextflow.k8s.model.PodHostMount
import nextflow.k8s.model.PodOptions

class CWSPodOptions extends PodOptions {

    private Collection<PodHostMount> hostMounts;

    CWSPodOptions(List<Map> options = null) {
        super(options)
        int size = options ? options.size() : 0
        hostMounts = new HashSet<>(size);
    }

    @Override
    void create(Map<String, String> entry) {
        if (entry.localPath && entry.mountPath) {
            hostMounts << new PodHostMount(entry.localPath, entry.mountPath)
        } else {
            super.create(entry)
        }
    }

    Collection<PodHostMount> getHostMounts() {
        return hostMounts
    }

    CWSPodOptions plus(CWSPodOptions other) {
        def result = new CWSPodOptions()

        // env vars
        result.envVars.addAll(envVars)
        result.envVars.addAll(other.envVars)

        // config maps
        result.mountConfigMaps.addAll(mountConfigMaps)
        result.mountConfigMaps.addAll(other.mountConfigMaps)

        // secrets
        result.mountSecrets.addAll(mountSecrets)
        result.mountSecrets.addAll(other.mountSecrets)

        // volume claims
        result.volumeClaims.addAll(volumeClaims)
        result.volumeClaims.addAll(other.volumeClaims)

        //host mounts
        result.hostMounts.addAll(hostMounts)
        result.hostMounts.addAll(other.hostMounts)

        // sec context
        if (other.securityContext)
            result.securityContext = other.securityContext
        else
            result.securityContext = securityContext

        // node selector
        result.nodeSelector = other.nodeSelector ?: this.nodeSelector

        // affinity
        result.affinity = other.affinity ?: this.affinity

        // pull policy
        if (other.imagePullPolicy)
            result.imagePullPolicy = other.imagePullPolicy
        else
            result.imagePullPolicy = imagePullPolicy

        // image secret
        if (other.imagePullSecret)
            result.imagePullSecret = other.imagePullSecret
        else
            result.imagePullSecret = imagePullSecret

        // labels
        result.labels.putAll(labels)
        result.labels.putAll(other.labels)

        // annotations
        result.annotations.putAll(annotations)
        result.annotations.putAll(other.annotations)

        // automount service account token
        result.automountServiceAccountToken = other.automountServiceAccountToken & this.automountServiceAccountToken

        // priority class name
        result.priorityClassName = other.priorityClassName ?: this.priorityClassName

        // tolerations
        result.tolerations = other.tolerations ?: this.tolerations

        //  privileged execution
        result.privileged = other.privileged != null ? other.privileged : this.privileged

        return result
    }
}