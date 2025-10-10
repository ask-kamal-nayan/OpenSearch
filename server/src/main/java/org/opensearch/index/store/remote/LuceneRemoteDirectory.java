/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.store.RemoteDirectory;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

/**
 * FormatRemoteDirectory implementation for Lucene format files.
 * Wraps existing RemoteDirectory to maintain full Lucene compatibility while adding format awareness.
 * 
 * This implementation follows the same pattern as LuceneStoreDirectory - it acts as an adapter
 * that delegates all operations to an existing RemoteDirectory while adding format-specific behavior.
 */
public class LuceneRemoteDirectory implements FormatRemoteDirectory {
    
    /**
     * Set of file extensions and patterns that Lucene format handles
     * This matches the patterns used in LuceneStoreDirectory for consistency
     */
    private static final Set<String> LUCENE_EXTENSIONS = Set.of(
        ".cfs", ".cfe", ".si", ".fnm", ".fdx", ".fdt", 
        ".tim", ".tip", ".doc", ".pos", ".pay", 
        ".nvd", ".nvm", ".dvm", ".dvd", ".tvx", ".tvd", ".tvf",
        ".del", ".liv"
    );
    
    private final DataFormat dataFormat;
    private final String remoteBasePath;
    private final RemoteDirectory wrappedRemoteDirectory;
    private final Logger logger;
    
    /**
     * Creates a LuceneRemoteDirectory that wraps an existing RemoteDirectory
     * 
     * @param baseRemotePath the base remote path (will become baseRemotePath/lucene)
     * @param wrappedRemoteDirectory existing RemoteDirectory to wrap and delegate to
     * @param logger logger for this directory
     */
    public LuceneRemoteDirectory(
        String baseRemotePath,
        RemoteDirectory wrappedRemoteDirectory,
        Logger logger
    ) {
        this.dataFormat = DataFormat.LUCENE;
        this.remoteBasePath = baseRemotePath + "/lucene";
        this.wrappedRemoteDirectory = wrappedRemoteDirectory;
        this.logger = logger;
        
        logger.debug("Created LuceneRemoteDirectory with remote base path: {}", this.remoteBasePath);
    }
    
    @Override
    public DataFormat getDataFormat() {
        return dataFormat;
    }
    
    @Override
    public String getRemoteBasePath() {
        return remoteBasePath;
    }
    
    @Override
    public boolean acceptsFile(String fileName) {
        // Accept files with Lucene extensions or specific Lucene file patterns
        boolean accepts = LUCENE_EXTENSIONS.stream().anyMatch(fileName::endsWith) ||
                         fileName.startsWith("segments_") ||
                         fileName.equals("write.lock");
        
        if (accepts) {
            logger.trace("LuceneRemoteDirectory accepts file: {}", fileName);
        }
        
        return accepts;
    }
    
    @Override
    public void initialize() throws IOException {
        logger.debug("Initializing LuceneRemoteDirectory for path: {}", remoteBasePath);
        // Lucene remote directory initialization - typically no special setup needed
        // The wrapped RemoteDirectory handles the actual remote storage initialization
    }
    
    @Override
    public void cleanup() throws IOException {
        logger.debug("Cleaning up LuceneRemoteDirectory for path: {}", remoteBasePath);
        // Lucene remote directory cleanup - typically no special cleanup needed
    }
    
    // Delegate all remote operations to the wrapped RemoteDirectory
    
    @Override
    public IndexOutput createOutput(String fileName, IOContext context) throws IOException {
        logger.trace("LuceneRemoteDirectory creating output for file: {}", fileName);
        return wrappedRemoteDirectory.createOutput(fileName, context);
    }
    
    @Override
    public boolean copyFrom(
        Directory from,
        String src,
        String remoteFileName,
        IOContext context,
        Runnable postUploadRunner,
        ActionListener<Void> listener,
        boolean lowPriorityUpload
    ) {
        logger.debug("LuceneRemoteDirectory copying {} to remote file {} (async)", src, remoteFileName);
        
        // Delegate directly to wrapped RemoteDirectory - this preserves all existing behavior
        // including multi-stream uploads, progress tracking, error handling, etc.
        return wrappedRemoteDirectory.copyFrom(
            from, src, remoteFileName, context, postUploadRunner, listener, lowPriorityUpload);
    }
    
    @Override
    public void copyFrom(Directory from, String src, String dest, IOContext context) throws IOException {
        logger.debug("LuceneRemoteDirectory copying {} to remote file {} (sync)", src, dest);
        
        // For sync copyFrom, we need to use the Directory interface
        // Since RemoteDirectory extends Directory, we can call copyFrom if available,
        // or implement file copy using createOutput/openInput pattern
        try {
            wrappedRemoteDirectory.copyFrom(from, src, dest, context);
        } catch (UnsupportedOperationException e) {
            // If wrapped directory doesn't support copyFrom, implement it manually
            logger.debug("Wrapped directory doesn't support sync copyFrom, implementing manually");
            
            try (IndexInput input = from.openInput(src, context);
                 IndexOutput output = wrappedRemoteDirectory.createOutput(dest, context)) {
                output.copyBytes(input, input.length());
            }
        }
    }
    
    @Override
    public IndexInput openInput(String fileName, IOContext context) throws IOException {
        logger.trace("LuceneRemoteDirectory opening input for file: {}", fileName);
        return wrappedRemoteDirectory.openInput(fileName, context);
    }
    
    @Override
    public IndexInput openInput(String fileName, long fileLength, IOContext context) throws IOException {
        logger.trace("LuceneRemoteDirectory opening input for file: {} with length: {}", fileName, fileLength);
        return wrappedRemoteDirectory.openInput(fileName, fileLength, context);
    }
    
    @Override
    public IndexInput openBlockInput(String fileName, long position, long length, long fileLength, IOContext context) throws IOException {
        logger.trace("LuceneRemoteDirectory opening block input for file: {} at position: {} length: {}", 
            fileName, position, length);
        return wrappedRemoteDirectory.openBlockInput(fileName, position, length, fileLength, context);
    }
    
    @Override
    public void deleteFile(String fileName) throws IOException {
        logger.debug("LuceneRemoteDirectory deleting file: {}", fileName);
        wrappedRemoteDirectory.deleteFile(fileName);
    }
    
    @Override
    public String[] listAll() throws IOException {
        logger.trace("LuceneRemoteDirectory listing all files");
        return wrappedRemoteDirectory.listAll();
    }
    
    @Override
    public Collection<String> listFilesByPrefix(String filenamePrefix) throws IOException {
        logger.trace("LuceneRemoteDirectory listing files with prefix: {}", filenamePrefix);
        return wrappedRemoteDirectory.listFilesByPrefix(filenamePrefix);
    }
    
    @Override
    public long fileLength(String fileName) throws IOException {
        logger.trace("LuceneRemoteDirectory getting length for file: {}", fileName);
        return wrappedRemoteDirectory.fileLength(fileName);
    }
    
    @Override
    public boolean fileExists(String fileName) throws IOException {
        logger.trace("LuceneRemoteDirectory checking existence of file: {}", fileName);
        try {
            wrappedRemoteDirectory.fileLength(fileName);
            return true;
        } catch (IOException e) {
            // If fileLength throws IOException, file doesn't exist
            return false;
        }
    }
    
    @Override
    public void delete() throws IOException {
        logger.debug("LuceneRemoteDirectory deleting entire directory: {}", remoteBasePath);
        wrappedRemoteDirectory.delete();
    }
    
    @Override
    public void close() throws IOException {
        logger.debug("LuceneRemoteDirectory closing directory: {}", remoteBasePath);
        wrappedRemoteDirectory.close();
    }
    
    /**
     * Returns the wrapped RemoteDirectory for direct access when needed
     * This follows the same pattern as LuceneStoreDirectory.getWrappedDirectory()
     * 
     * @return the wrapped RemoteDirectory instance
     */
    public RemoteDirectory getWrappedRemoteDirectory() {
        return wrappedRemoteDirectory;
    }
    
    @Override
    public String toString() {
        return "LuceneRemoteDirectory{" +
               "remoteBasePath='" + remoteBasePath + '\'' +
               ", wrappedDirectory=" + wrappedRemoteDirectory.getClass().getSimpleName() +
               '}';
    }
}
