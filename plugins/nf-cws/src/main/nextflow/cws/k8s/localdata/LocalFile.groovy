package nextflow.cws.k8s.localdata

import java.nio.file.Path

class LocalFile extends File {

    private final LocalPath localPath;

    LocalFile( LocalPath localPath ){
        super( localPath.toString() )
        this.localPath = localPath;
    }

    @Override
    Path toPath() {
        return localPath
    }
}