package nextflow.cws.k8s.localdata

import groovy.util.logging.Slf4j
import nextflow.file.FileHelper
import nextflow.cws.wow.file.LocalFileWalker
import nextflow.cws.k8s.K8sSchedulerClient
import org.codehaus.groovy.runtime.IOGroovyMethods
import sun.net.ftp.FtpClient

import java.nio.charset.Charset
import java.nio.file.*
import java.nio.file.attribute.BasicFileAttributes

@Slf4j
class LocalPath implements Path {

    private final Path path
    private transient final LocalFileWalker.FileAttributes attributes
    private static transient K8sSchedulerClient client = null
    private boolean wasDownloaded = false
    private Path workDir
    private boolean createdSymlinks = false
    private transient final Object createSymlinkHelper = new Object()

    private LocalPath(Path path, LocalFileWalker.FileAttributes attributes, Path workDir ) {
        this.path = path
        this.attributes = attributes
        this.workDir = workDir
    }

    private LocalPath(){
        path = null
        this.attributes = null
        this.workDir = null
    }

    LocalPath toLocalPath( Path path, LocalFileWalker.FileAttributes attributes = null ){
        toLocalPath( path, attributes, workDir )
    }

    static LocalPath toLocalPath( Path path, LocalFileWalker.FileAttributes attributes, Path workDir ){
        ( path instanceof LocalPath ) ? path as LocalPath : new LocalPath( path, attributes, workDir )
    }

    static void setClient( K8sSchedulerClient client ){
        if ( !this.client ) this.client = client
        else throw new IllegalStateException("Client was already set.")
    }

    private FtpClient getConnection( final String node, String daemon ){
        int trial = 0
        while ( true ) {
            try {
                FtpClient ftpClient = FtpClient.create(daemon)
                ftpClient.login("root", "password".toCharArray() )
                ftpClient.enablePassiveMode( true )
                return ftpClient
            } catch ( IOException e ) {
                if ( trial > 5 ) throw e
                log.error("Cannot create FTP client: $daemon on $node", e)
                sleep(Math.pow(2, trial++) as long)
                daemon = client.getDaemonOnNode(node)
            }
        }
    }

    private Map getLocation( String absolutePath ){
        Map response = client.getFileLocation( absolutePath )
        synchronized ( createSymlinkHelper ) {
            if ( !createdSymlinks ) {
                for ( Map link : (response.symlinks as List<Map>)) {
                    Path src = link.src as Path
                    Path dst = link.dst as Path
                    if (Files.exists(src, LinkOption.NOFOLLOW_LINKS)) {
                        try {
                            if (src.isDirectory()) src.deleteDir()
                            else Files.delete(src)
                        } catch ( Exception ignored){
                            log.warn( "Unable to delete " + src )
                        }
                    } else {
                        src.parent.toFile().mkdirs()
                    }
                    try{
                        Files.createSymbolicLink(src, dst)
                    } catch ( Exception ignored){
                        log.warn( "Unable to create symlink: "  + src + " -> " + dst )
                    }
                }
                createdSymlinks = true
            }
        }
        response
    }

    private void checkParentDirectoryExists() {
        if (!path.getParent().exists()) {
            try {
                log.trace("Creating directory locally ${path.getParent().toAbsolutePath().toString()}")
                Files.createDirectories(path.getParent())
            } catch (IOException e) {
                log.error("Couldn't create directory ${path.getParent().toAbsolutePath().toString()}", e)
            }
        }
    }

    /**
     *
     * @return A path located in the original workdir
     */
    Path fakePath(){
        Path fake = FileHelper.fakePath( path, workDir )
        return fake
    }

    String getText(){
        getText( Charset.defaultCharset().toString() )
    }

    String getText( String charset ){
        final String absolutePath = path.toAbsolutePath().toString()
        final def location = getLocation( absolutePath )
        if ( wasDownloaded || location.sameAsEngine ){
            log.trace("Read locally $absolutePath")
            return path.getText( charset )
        }
        try (FtpClient ftpClient = getConnection(location.node as String, location.daemon as String)) {
            try (InputStream fileStream = ftpClient.getFileStream(location.path as String)) {
                log.trace("Read remote $absolutePath")
                return fileStream.getText( charset )
            }
        }
    }

    void setText( String text ){
        setText( text, Charset.defaultCharset().toString() )
    }

    void setText( String text, String charset ){
        final String absolutePath = path.toAbsolutePath().toString()
        final def location = client.getFileLocation( absolutePath )
        checkParentDirectoryExists()
        log.trace("Write locally $absolutePath")
        path.setText( text, charset )
        def file = path.toFile()
        client.addFileLocation( path.toString(), file.size(), file.lastModified(), location.locationWrapperID as long, true )
    }

    byte[] getBytes(){
        final String absolutePath = path.toAbsolutePath().toString()
        final def location = getLocation( absolutePath )
        if ( wasDownloaded || location.sameAsEngine ){
            log.trace("Read locally $absolutePath")
            return path.getBytes()
        }
        try (FtpClient ftpClient = getConnection(location.node as String, location.daemon as String)) {
            try (InputStream fileStream = ftpClient.getFileStream( location.path as String )) {
                log.trace("Read remote $absolutePath")
                return fileStream.getBytes()
            }
        }
    }

    void setBytes( byte[] bytes ){
        final String absolutePath = path.toAbsolutePath().toString()
        final def location = client.getFileLocation( absolutePath )
        checkParentDirectoryExists()
        log.trace("Write locally $absolutePath")
        path.setBytes( bytes )
        def file = path.toFile()
        client.addFileLocation( path.toString(), file.size(), file.lastModified(), location.locationWrapperID as long, true )
    }

    Object withReader( Closure closure ){
        withReader ( Charset.defaultCharset().toString(), closure )
    }

    Object withReader( String charset, Closure closure ){
        final String absolutePath = path.toAbsolutePath().toString()
        final def location = getLocation( absolutePath )
        if ( wasDownloaded || location.sameAsEngine ){
            log.trace("Read locally $absolutePath")
            return path.withReader( charset, closure )
        }
        try (FtpClient ftpClient = getConnection( location.node as String , location.daemon as String )) {
            try (InputStream fileStream = ftpClient.getFileStream( location.path as String )) {
                log.trace("Read remote $absolutePath")
                return IOGroovyMethods.withReader(fileStream, closure)
            }
        }
    }

    def <T> T withWriter( Closure<T> closure ){
        return withWriter( Charset.defaultCharset().toString(), closure )
    }

    def <T> T withWriter( String charset, Closure<T> closure ) {
        final String absolutePath = path.toAbsolutePath().toString()
        final def location = client.getFileLocation( absolutePath )
        checkParentDirectoryExists()
        log.trace("Write locally $absolutePath")
        T rv = path.withWriter( charset, closure )
        def file = path.toFile()
        client.addFileLocation( path.toString(), file.size(), file.lastModified(), location.locationWrapperID as long, true )
        return rv
    }

    def <T> T withPrintWriter( String charset, Closure<T> closure ){
        final String absolutePath = path.toAbsolutePath().toString()
        final def location = client.getFileLocation( absolutePath )
        checkParentDirectoryExists()
        log.trace("Write locally $absolutePath")
        T rv = path.withPrintWriter( charset, closure )
        def file = path.toFile()
        client.addFileLocation( path.toString(), file.size(), file.lastModified(), location.locationWrapperID as long, true )
        return rv
    }

    List<String> readLines(){
        IOGroovyMethods.readLines( newReader() )
    }

    List<String> readLines( String charset ){
        IOGroovyMethods.readLines( newReader( charset ) )
    }

    public <T> T eachLine( Closure<T> closure ) throws IOException {
        eachLine( Charset.defaultCharset().toString(), 1, closure )
    }

    public <T> T eachLine( int firstLine, Closure<T> closure ) throws IOException {
        eachLine( Charset.defaultCharset().toString(), firstLine, closure )
    }

    public <T> T eachLine( String charset, Closure<T> closure ) throws IOException {
        eachLine( charset, 1, closure )
    }

    public <T> T eachLine( String charset, int firstLine, Closure<T> closure ) throws IOException {
        final String absolutePath = path.toAbsolutePath().toString()
        final def location = getLocation( absolutePath )
        if ( wasDownloaded || location.sameAsEngine ){
            log.trace("Read locally $absolutePath")
            return path.eachLine( charset, firstLine, closure )
        }
        try (FtpClient ftpClient = getConnection( location.node as String, location.daemon as String )) {
            try (InputStream fileStream = ftpClient.getFileStream( location.path as String )) {
                log.trace("Read remote $absolutePath")
                return IOGroovyMethods.eachLine( fileStream, charset, firstLine, closure )
            }
        }
    }

    BufferedReader newReader(){
        return newReader( Charset.defaultCharset().toString() )
    }

    BufferedReader newReader( String charset ){
        final String absolutePath = path.toAbsolutePath().toString()
        final def location = getLocation( absolutePath )
        if ( wasDownloaded || location.sameAsEngine ){
            log.trace("Read locally $absolutePath")
            return path.newReader()
        }
        try (FtpClient ftpClient = getConnection( location.node as String , location.daemon as String )) {
            InputStream fileStream = ftpClient.getFileStream( location.path as String )
            log.trace("Read remote $absolutePath")
            InputStreamReader isr = new InputStreamReader( fileStream, charset )
            return new BufferedReader(isr)
        }
    }

    BufferedWriter newWriter(){
        return newWriter( Charset.defaultCharset().toString(), false, false )
    }

    BufferedWriter newWriter( String charset ) {
        return newWriter( charset, false, false )
    }

    BufferedWriter newWriter( boolean append ) {
        return newWriter( Charset.defaultCharset().toString(), append, false )
    }

    BufferedWriter newWriter( String charset, boolean append ){
        return newWriter( charset, append, false )
    }

    BufferedWriter newWriter( String charset, boolean append, boolean writeBom ){
        final String absolutePath = path.toAbsolutePath().toString()
        final def location = client.getFileLocation( absolutePath )
        checkParentDirectoryExists()
        log.trace("Write locally $absolutePath")
        BufferedWriter bufferedWriter = path.newWriter( charset, append, writeBom )
        def file = path.toFile()
        client.addFileLocation( path.toString(), file.size(), file.lastModified(), location.locationWrapperID as long, true )
        return bufferedWriter
    }

    PrintWriter newPrintWriter( String charset ){
        final String absolutePath = path.toAbsolutePath().toString()
        final def location = client.getFileLocation( absolutePath )
        checkParentDirectoryExists()
        log.trace("Write locally $absolutePath")
        PrintWriter printWriter = path.newPrintWriter( charset )
        def file = path.toFile()
        client.addFileLocation( path.toString(), file.size(), file.lastModified(), location.locationWrapperID as long, true )
        return printWriter
    }

    public <T> T eachByte( Closure<T> closure ) throws IOException {
        final String absolutePath = path.toAbsolutePath().toString()
        final def location = getLocation( absolutePath )
        if ( wasDownloaded || location.sameAsEngine ){
            log.trace("Read locally $absolutePath")
            return path.eachByte( closure )
        }
        try (FtpClient ftpClient = getConnection( location.node as String , location.daemon as String )) {
            try (InputStream fileStream = ftpClient.getFileStream( location.path as String )) {
                log.trace("Read remote $absolutePath")
                return IOGroovyMethods.eachByte( new BufferedInputStream( fileStream ), closure)
            }
        }
    }

    public <T> T eachByte( int bufferLen, Closure<T> closure ) throws IOException {
        final String absolutePath = path.toAbsolutePath().toString()
        final def location = getLocation( absolutePath )
        if ( wasDownloaded || location.sameAsEngine ){
            log.trace("Read locally $absolutePath")
            return path.eachByte( bufferLen, closure )
        }
        try (FtpClient ftpClient = getConnection( location.node as String , location.daemon as String )) {
            try (InputStream fileStream = ftpClient.getFileStream( location.path as String )) {
                log.trace("Read remote $absolutePath")
                return IOGroovyMethods.eachByte( new BufferedInputStream( fileStream ), bufferLen, closure)
            }
        }
    }

    public <T> T withInputStream( Closure<T> closure) throws IOException {
        final String absolutePath = path.toAbsolutePath().toString()
        final def location = getLocation( absolutePath )
        if ( wasDownloaded || location.sameAsEngine ){
            log.trace("Read locally $absolutePath")
            return path.withInputStream( closure )
        }
        try (FtpClient ftpClient = getConnection( location.node as String , location.daemon as String )) {
            InputStream fileStream = ftpClient.getFileStream( location.path as String )
            log.trace("Read remote $absolutePath")
            return IOGroovyMethods.withStream(new BufferedInputStream( fileStream ), closure)
        }
    }

    def <T> T withOutputStream( Closure<T> closure ){
        final String absolutePath = path.toAbsolutePath().toString()
        final def location = client.getFileLocation( absolutePath )
        checkParentDirectoryExists()
        log.trace("Write locally $absolutePath")
        T rv = path.withOutputStream( closure )
        def file = path.toFile()
        client.addFileLocation( path.toString(), file.size(), file.lastModified(), location.locationWrapperID as long, true )
        return rv
    }

    public BufferedInputStream newInputStream() throws IOException {
        final String absolutePath = path.toAbsolutePath().toString()
        final def location = getLocation( absolutePath )
        if ( wasDownloaded || location.sameAsEngine ){
            log.trace("Read locally $absolutePath")
            return path.newInputStream()
        }
        try (FtpClient ftpClient = getConnection( location.node as String , location.daemon as String )) {
            InputStream fileStream = ftpClient.getFileStream( location.path as String )
            log.trace("Read remote $absolutePath")
            return new BufferedInputStream( fileStream )
        }
    }

    BufferedOutputStream newOutputStream(){
        final String absolutePath = path.toAbsolutePath().toString()
        final def location = client.getFileLocation( absolutePath )
        checkParentDirectoryExists()
        log.trace("Write locally $absolutePath")
        BufferedOutputStream bufferedOutputStream = path.newOutputStream()
        def file = path.toFile()
        client.addFileLocation( path.toString(), file.size(), file.lastModified(), location.locationWrapperID as long, true )
        return bufferedOutputStream
    }

    void write( String text ){
        write(text, Charset.defaultCharset().toString(), false)
    }

    void write( String text, String charset ){
        write(text, charset, false)
    }

    void write( String text, boolean writeBom ){
        write(text, Charset.defaultCharset().toString(), writeBom)
    }

    void write( String text, String charset, boolean writeBom ){
        final String absolutePath = path.toAbsolutePath().toString()
        final def location = client.getFileLocation( absolutePath )
        checkParentDirectoryExists()
        log.trace("Write locally $absolutePath")
        path.write( text, charset, writeBom )
        def file = path.toFile()
        client.addFileLocation( path.toString(), file.size(), file.lastModified(), location.locationWrapperID as long, true )
        def loc = client.getFileLocation( absolutePath )
    }

    void leftShift(Object text) {
        append(text)
    }

    void append( Object text ){
        append(text, Charset.defaultCharset().toString(), false)
    }

    void append( Object text, String charset ){
        append(text, charset, false)
    }

    void append( Object text, boolean writeBom ){
        append(text, Charset.defaultCharset().toString(), writeBom)
    }

    void append( Object text, String charset, boolean writeBom ){
        if ( !text ) {
            return
        }
        final String absolutePath = path.toAbsolutePath().toString()
        final def location = client.getFileLocation( absolutePath )
        if ( wasDownloaded || location.sameAsEngine ) {
            log.info("Append locally $absolutePath")
            path.append( text, charset, writeBom )
        } else {
            try (FtpClient ftpClient = getConnection(location.node.toString(), location.daemon.toString())) {
                byte[] bytes = text.toString().getBytes( charset )
                InputStream stream = new ByteArrayInputStream( bytes )
                log.info("Append remote $absolutePath")
                ftpClient.appendFile( absolutePath, stream )
            }
        }
        def file = path.toFile()
        client.addFileLocation( path.toString(), file.size(), file.lastModified(), location.locationWrapperID as long, true )
        def loc = client.getFileLocation( absolutePath )
    }

    private Map download(){
        final String absolutePath = path.toAbsolutePath().toString()
        final def location = getLocation( absolutePath )
        synchronized ( this ) {
            if ( this.wasDownloaded || location.sameAsEngine ) {
                log.trace("No download")
                return [ wasDownloaded : false, location : location ]
            }
            try (FtpClient ftpClient = getConnection(location.node as String, location.daemon as String )) {
                try (InputStream fileStream = ftpClient.getFileStream( location.path as String )) {
                    log.trace("Download remote $absolutePath")
                    final def file = toFile()
                    path.parent.toFile().mkdirs()
                    OutputStream outStream = new FileOutputStream(file)
                    byte[] buffer = new byte[8 * 1024]
                    int bytesRead
                    while ((bytesRead = fileStream.read(buffer)) != -1) {
                        outStream.write(buffer, 0, bytesRead)
                    }
                    fileStream.closeQuietly()
                    outStream.closeQuietly()
                    this.wasDownloaded = true
                    return [ wasDownloaded : true, location : location ]
                } catch (Exception e) {
                    throw e
                }
            } catch (Exception e) {
                throw e
            }
        }
    }

    <T> T asType( Class<T> c ) {
        if ( c == Path.class ) return this
        if ( c == File.class ) return toFile()
        if ( c == String.class ) return toString()
        log.info("Invoke method asType $c on $this")
        return super.asType( c )
    }

    @Override
    Object invokeMethod(String name, Object args) {
        Map downloadResult = download()
        def file = path.toFile()
        def lastModified = file.lastModified()
        Object result = path.invokeMethod(name, args)
        if( lastModified != file.lastModified() ){
            //Update location in scheduler (overwrite all others)
            client.addFileLocation( downloadResult.location.path as String , file.size(), file.lastModified(), downloadResult.location.locationWrapperID as long, true )
        } else if ( downloadResult.wasDownloaded ){
            //Add location to scheduler
            client.addFileLocation( downloadResult.location.path as String , file.size(), file.lastModified(), downloadResult.location.locationWrapperID as long, false )
        }
        return result
    }

    String getBaseName() {
        path.getBaseName()
    }

    boolean isDirectory( LinkOption... options ) {
        attributes ? attributes.isDirectory() : 0
    }

    long size() {
        attributes ? attributes.size() : 0
    }

    boolean empty(){
        //TODO empty file?
        this.size() == 0
    }

    boolean asBoolean(){
        true
    }

    @Override
    FileSystem getFileSystem() {
        LocalFileSystem.INSTANCE
    }

    @Override
    boolean isAbsolute() {
        path.isAbsolute()
    }

    @Override
    Path getRoot() {
        path.getRoot()
    }

    @Override
    Path getFileName() {
        path.getFileName()
    }

    @Override
    Path getParent() {
        toLocalPath( path.getParent() )
    }

    @Override
    int getNameCount() {
        path.getNameCount()
    }

    @Override
    Path getName(int index) {
        path.getName( index )
    }

    @Override
    Path subpath(int beginIndex, int endIndex) {
        toLocalPath( path.subpath( beginIndex, endIndex ) )
    }

    @Override
    boolean startsWith(Path other) {
        path.startsWith( other )
    }

    @Override
    boolean startsWith(String other) {
        path.startsWith(other)
    }

    @Override
    boolean endsWith(Path other) {
        path.endsWith( other )
    }

    @Override
    boolean endsWith(String other) {
        path.endsWith( other )
    }

    @Override
    Path normalize() {
        toLocalPath( path.normalize() )
    }

    @Override
    Path resolve(Path other) {
        //TODO other attributes
        toLocalPath( path.resolve( other ) )
    }

    @Override
    Path resolve(String other) {
        //TODO other attributes
        toLocalPath( path.resolve( other ) )
    }

    @Override
    Path resolveSibling(Path other) {
        path.resolveSibling( other )
    }

    @Override
    Path resolveSibling(String other) {
        path.resolveSibling( other )
    }

    @Override
    Path relativize(Path other) {
        path.relativize( other )
    }

    @Override
    URI toUri() {
        path.toUri()
    }

    Path toAbsolutePath(){
        toLocalPath( path.toAbsolutePath() )
    }

    @Override
    Path toRealPath(LinkOption... options) throws IOException {
        attributes.destination ? toLocalPath( attributes.destination ) : toLocalPath( path.toRealPath( options ) )
    }

    @Override
    File toFile() {
        path.toFile()
    }

    @Override
    WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers) throws IOException {
        path.register( watcher, events, modifiers )
    }

    @Override
    WatchKey register(WatchService watcher, WatchEvent.Kind<?>... events) throws IOException {
        path.register( watcher, events )
    }

    @Override
    int compareTo(Path other) {
        if ( other instanceof LocalPath ){
            return path.compareTo( ((LocalPath) other).path )
        }
        path.compareTo( other )
    }

    @Override
    String toString() {
        path.toString()
    }

    BasicFileAttributes getAttributes(){
        attributes
    }
}