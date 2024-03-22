package nextflow.cws.k8s.client

import groovy.util.logging.Slf4j
import groovy.json.JsonOutput
import nextflow.k8s.client.ClientConfig
import nextflow.k8s.client.K8sClient
import nextflow.k8s.client.K8sResponseJson
import org.yaml.snakeyaml.Yaml

import java.nio.file.Path

@Slf4j
class CWSK8sClient extends K8sClient {

    CWSK8sClient(ClientConfig config) {
        super(config)
    }

    K8sResponseJson podCreate(String req, namespace = config.namespace) {
        assert req
        final action = "/api/v1/namespaces/$namespace/pods"
        final resp = post(action, req)
        trace('POST', action, resp.text)
        return new K8sResponseJson(resp.text)
    }

    K8sResponseJson podCreate(Map req, Path saveYamlPath=null, namespace = config.namespace) {

        if( saveYamlPath ) try {
            saveYamlPath.text = new Yaml().dump(req).toString()
        }
        catch( Exception e ) {
            log.debug "WARN: unable to save request yaml -- cause: ${e.message ?: e}"
        }

        podCreate(JsonOutput.toJson(req), namespace)
    }

    K8sResponseJson daemonSetCreate(Map req, Path saveYamlPath=null) {

        if( saveYamlPath )
            try {
                saveYamlPath.text = new Yaml().dump(req).toString()
            }
            catch( Exception e ) {
                log.debug "WARN: unable to save request yaml -- cause: ${e.message ?: e}"
            }

        daemonSetCreate(JsonOutput.toJson(req))
    }

    K8sResponseJson daemonSetCreate(String req) {
        assert req
        final action = "/apis/apps/v1/namespaces/$config.namespace/daemonsets"
        final resp = post(action, req)
        trace('POST', action, resp.text)
        new K8sResponseJson(resp.text)
    }

    K8sResponseJson daemonSetDelete(String name) {
        assert name
        final action = "/apis/apps/v1/namespaces/$config.namespace/daemonsets/$name"
        final resp = delete(action)
        trace('DELETE', action, resp.text)
        new K8sResponseJson(resp.text)
    }

    K8sResponseJson configCreateBinary(String name, Map data) {
        final spec = [
                apiVersion: 'v1',
                kind: 'ConfigMap',
                metadata: [ name: name, namespace: config.namespace ],
                binaryData: data
        ]
        configCreate0(spec)
    }
}
