package nextflow.cws.wow.fs

import java.nio.file.FileStore
import java.nio.file.FileSystem
import java.nio.file.Path
import java.nio.file.PathMatcher
import java.nio.file.WatchService
import java.nio.file.attribute.UserPrincipalLookupService
import java.nio.file.spi.FileSystemProvider

class WOWFileSystem extends FileSystem {

    static final WOWFileSystem INSTANCE = new WOWFileSystem()

    @Override
    FileSystemProvider provider() {
        return WOWFileSystemProvider.INSTANCE
    }

    @Override
    void close() throws IOException {
    }

    @Override
    boolean isOpen() {
        return true
    }

    @Override
    boolean isReadOnly() {
        return false
    }

    @Override
    String getSeparator() {
        return "/"
    }

    @Override
    Iterable<Path> getRootDirectories() {
        throw new UnsupportedOperationException("Root directories not supported by ${provider().getScheme().toUpperCase()} file system")
    }

    @Override
    Iterable<FileStore> getFileStores() {
        throw new UnsupportedOperationException("File stores not supported by ${provider().getScheme().toUpperCase()} file system")
    }

    @Override
    Set<String> supportedFileAttributeViews() {
        return new HashSet<>()
    }

    @Override
    Path getPath(String s, String... strings) {
        throw new UnsupportedOperationException("Path get not supported by ${provider().getScheme().toUpperCase()} file system")
    }

    @Override
    PathMatcher getPathMatcher(String s) {
        throw new UnsupportedOperationException("Path matcher not supported by ${provider().getScheme().toUpperCase()} file system")
    }

    @Override
    UserPrincipalLookupService getUserPrincipalLookupService() {
        throw new UnsupportedOperationException("User principal lookup service not supported by ${provider().getScheme().toUpperCase()} file system")
    }

    @Override
    WatchService newWatchService() throws IOException {
        throw new UnsupportedOperationException("Watch service not supported by ${provider().getScheme().toUpperCase()} file system")
    }
}
