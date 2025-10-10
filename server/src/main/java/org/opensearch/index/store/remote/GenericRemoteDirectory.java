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
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.blobstore.transfer.RemoteTransferContainer;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeIndexInputStream;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.store.RemoteDirectory;
import org.opensearch.index.store.RemoteIndexInput;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * Generic FormatRemoteDirectory implementation for non-Lucene formats.
 * Uses BlobContainer directly to provide remote directory functionality without Lucene dependencies.
 * 
 * This implementation creates format-specific remote paths and handles format-specific file operations
 * while maintaining compatibility with the existing remote storage infrastructure.
 */
public class GenericRemoteDirectory implements FormatRemoteDirectory {
    
    private final DataFormat dataFormat;
    private final String remoteBasePath;
    private final BlobContainer blobContainer;
    private final Set<String> acceptedExtensions;
    private final Logger logger;
    private final UnaryOperator<InputStream> downloadRateLimiter;
    private final UnaryOperator<OffsetRangeIndexInputStream> uploadRateLimiter;
    private final UnaryOperator<OffsetRangeIndexInputStream> lowPriorityUploadRateLimiter;
    
    /**
     * Creates a GenericRemoteDirectory for non-Lucene formats
     * 
     * @param dataFormat the data format this directory handles
     * @param baseRemotePath the base remote path (will become baseRemotePath/{format})
     * @param blobContainer the blob container for remote storage operations
     * @param acceptedExtensions set of file extensions this directory accepts
     * @param logger logger for this directory
     */
    public GenericRemoteDirectory(
        DataFormat dataFormat,
        String baseRemotePath,
        BlobContainer blobContainer,
        Set<String> acceptedExtensions,
        Logger logger
    ) {
        this(dataFormat, baseRemotePath, blobContainer, acceptedExtensions, logger,
             UnaryOperator.identity(), UnaryOperator.identity(), UnaryOperator.identity());
    }
    
    /**
     * Creates a GenericRemoteDirectory with rate limiters
     */
    public GenericRemoteDirectory(
        DataFormat dataFormat,
        String baseRemotePath,
        BlobContainer blobContainer,
        Set<String> acceptedExtensions,
        Logger logger,
        UnaryOperator<OffsetRangeIndexInputStream> uploadRateLimiter,
        UnaryOperator<OffsetRangeIndexInputStream> lowPriorityUploadRateLimiter,
        UnaryOperator<InputStream> downloadRateLimiter
    ) {
        this.dataFormat = dataFormat;
        this.remoteBasePath = baseRemotePath + "/" + dataFormat.getDirectoryName();
        this.blobContainer = blobContainer;
        this.acceptedExtensions = Set.copyOf(acceptedExtensions);
        this.logger = logger;
        this.uploadRateLimiter = uploadRateLimiter;
        this.lowPriorityUploadRateLimiter = lowPriorityUploadRateLimiter;
        this.downloadRateLimiter = downloadRateLimiter;
        
        logger.debug("Created GenericRemoteDirectory for format: {} with remote base path: {}", 
            dataFormat.name(), this.remoteBasePath);
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
        boolean accepts = acceptedExtensions.stream().anyMatch(fileName::endsWith);
        
        if (accepts) {
            logger.trace("GenericRemoteDirectory ({}) accepts file: {}", dataFormat.name(), fileName);
        }
        
        return accepts;
    }
    
    @Override
    public void initialize() throws IOException {
        logger.debug("Initializing GenericRemoteDirectory for format: {} path: {}", 
            dataFormat.name(), remoteBasePath);
        // Generic remote directory initialization - format-specific setup can be added here
    }
    
    @Override
    public void cleanup() throws IOException {
        logger.debug("Cleaning up GenericRemoteDirectory for format: {} path: {}", 
            dataFormat.name(), remoteBasePath);
        // Generic remote directory cleanup - format-specific cleanup can be added here
    }
    
    @Override
    public IndexOutput createOutput(String fileName, IOContext context) throws IOException {
        logger.trace("GenericRemoteDirectory ({}) creating output for file: {}", dataFormat.name(), fileName);
        
        // For generic remote directories, we create an IndexOutput that writes to BlobContainer
        // This is a simplified implementation - in practice, this might need more sophisticated buffering
        return new BlobContainerIndexOutput(fileName, blobContainer, logger);
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
        logger.debug("GenericRemoteDirectory ({}) copying {} to remote file {} (async)", 
            dataFormat.name(), src, remoteFileName);
        
        // Check if we can use multi-stream upload
        if (blobContainer instanceof AsyncMultiStreamBlobContainer) {
            try {
                uploadBlobAsync(from, src, remoteFileName, context, postUploadRunner, listener, lowPriorityUpload);
                return true;
            } catch (Exception e) {
                logger.error("Exception in async upload for file: {} format: {}", src, dataFormat.name(), e);
                listener.onFailure(e);
                return true; // We handled the request even if it failed
            }
        }
        
        // Return false to indicate caller should use sync copyFrom
        return false;
    }
    
    @Override
    public void copyFrom(Directory from, String src, String dest, IOContext context) throws IOException {
        logger.debug("GenericRemoteDirectory ({}) copying {} to remote file {} (sync)", 
            dataFormat.name(), src, dest);
        
        // Implement synchronous file copy using IndexInput/OutputStream
        try (IndexInput input = from.openInput(src, context)) {
            long contentLength = input.length();
            
            // Convert IndexInput to InputStream for BlobContainer upload
            InputStream inputStream = new IndexInputInputStream(input);
            
            // Upload via BlobContainer
            blobContainer.writeBlob(dest, inputStream, contentLength, false);
            
            logger.debug("Successfully uploaded {} format file {} to remote path: {}/{}", 
                dataFormat.name(), src, remoteBasePath, dest);
        }
    }
    
    @Override
    public IndexInput openInput(String fileName, IOContext context) throws IOException {
        logger.trace("GenericRemoteDirectory ({}) opening input for file: {}", dataFormat.name(), fileName);
        
        // Get file length first
        long fileLength = fileLength(fileName);
        return openInput(fileName, fileLength, context);
    }
    
    @Override
    public IndexInput openInput(String fileName, long fileLength, IOContext context) throws IOException {
        logger.trace("GenericRemoteDirectory ({}) opening input for file: {} with length: {}", 
            dataFormat.name(), fileName, fileLength);
        
        try {
            InputStream inputStream = blobContainer.readBlob(fileName);
            return new RemoteIndexInput(fileName, downloadRateLimiter.apply(inputStream), fileLength);
        } catch (Exception e) {
            logger.error("Exception while reading blob for file: {} format: {} path: {}", 
                fileName, dataFormat.name(), blobContainer.path(), e);
            throw e;
        }
    }
    
    @Override
    public IndexInput openBlockInput(String fileName, long position, long length, long fileLength, IOContext context) throws IOException {
        logger.trace("GenericRemoteDirectory ({}) opening block input for file: {} at position: {} length: {}", 
            dataFormat.name(), fileName, position, length);
        
        if (position < 0 || length <= 0 || (position + length > fileLength)) {
            throw new IllegalArgumentException("Invalid values of block start and size");
        }
        
        try (InputStream inputStream = blobContainer.readBlob(fileName, position, length)) {
            byte[] bytes = downloadRateLimiter.apply(inputStream).readAllBytes();
            return new ByteArrayIndexInput(fileName, bytes);
        } catch (Exception e) {
            logger.error("Exception while reading block for file: {} format: {} path: {}", 
                fileName, dataFormat.name(), blobContainer.path(), e);
            throw e;
        }
    }
    
    @Override
    public void deleteFile(String fileName) throws IOException {
        logger.debug("GenericRemoteDirectory ({}) deleting file: {}", dataFormat.name(), fileName);
        blobContainer.deleteBlobsIgnoringIfNotExists(Collections.singletonList(fileName));
    }
    
    @Override
    public String[] listAll() throws IOException {
        logger.trace("GenericRemoteDirectory ({}) listing all files", dataFormat.name());
        return blobContainer.listBlobs().keySet().stream().sorted().toArray(String[]::new);
    }
    
    @Override
    public Collection<String> listFilesByPrefix(String filenamePrefix) throws IOException {
        logger.trace("GenericRemoteDirectory ({}) listing files with prefix: {}", dataFormat.name(), filenamePrefix);
        return blobContainer.listBlobsByPrefix(filenamePrefix).keySet();
    }
    
    @Override
    public long fileLength(String fileName) throws IOException {
        logger.trace("GenericRemoteDirectory ({}) getting length for file: {}", dataFormat.name(), fileName);
        
        // Use the same pattern as RemoteDirectory.fileLength()
        List<BlobMetadata> metadata = blobContainer.listBlobsByPrefixInSortedOrder(
            fileName, 1, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC);
        
        if (metadata.size() == 1 && metadata.get(0).name().equals(fileName)) {
            return metadata.get(0).length();
        }
        
        throw new NoSuchFileException(fileName);
    }
    
    @Override
    public boolean fileExists(String fileName) throws IOException {
        logger.trace("GenericRemoteDirectory ({}) checking existence of file: {}", dataFormat.name(), fileName);
        
        try {
            fileLength(fileName);
            return true;
        } catch (NoSuchFileException e) {
            return false;
        }
    }
    
    @Override
    public void delete() throws IOException {
        logger.debug("GenericRemoteDirectory ({}) deleting entire directory: {}", dataFormat.name(), remoteBasePath);
        blobContainer.delete();
    }
    
    @Override
    public void close() throws IOException {
        logger.debug("GenericRemoteDirectory ({}) closing directory: {}", dataFormat.name(), remoteBasePath);
        // BlobContainer typically doesn't require explicit closing
    }
    
    /**
     * Async blob upload implementation for generic formats
     * This follows the same pattern as RemoteDirectory.uploadBlob()
     */
    private void uploadBlobAsync(
        Directory from,
        String src,
        String remoteFileName,
        IOContext context,
        Runnable postUploadRunner,
        ActionListener<Void> listener,
        boolean lowPriorityUpload
    ) throws Exception {
        
        // Calculate content length and checksum
        long contentLength;
        try (IndexInput indexInput = from.openInput(src, context)) {
            contentLength = indexInput.length();
        }
        
        // Determine if this should be low priority
        lowPriorityUpload = lowPriorityUpload || contentLength > ByteSizeUnit.GB.toBytes(15);
        
        // Create transfer container for multi-stream upload
        RemoteTransferContainer.OffsetRangeInputStreamSupplier inputStreamSupplier;
        if (lowPriorityUpload) {
            inputStreamSupplier = (size, position) -> lowPriorityUploadRateLimiter.apply(
                new OffsetRangeIndexInputStream(from.openInput(src, context), size, position));
        } else {
            inputStreamSupplier = (size, position) -> uploadRateLimiter.apply(
                new OffsetRangeIndexInputStream(from.openInput(src, context), size, position));
        }
        
        // For generic formats, we use a simple checksum approach
        long expectedChecksum = 0; // Generic formats may not need complex checksum validation
        
        boolean remoteIntegrityEnabled = false;
        if (blobContainer instanceof AsyncMultiStreamBlobContainer) {
            remoteIntegrityEnabled = ((AsyncMultiStreamBlobContainer) blobContainer).remoteIntegrityCheckSupported();
        }
        
        RemoteTransferContainer remoteTransferContainer = new RemoteTransferContainer(
            src,
            remoteFileName,
            contentLength,
            true,
            lowPriorityUpload ? WritePriority.LOW : WritePriority.NORMAL,
            inputStreamSupplier,
            expectedChecksum,
            remoteIntegrityEnabled
        );
        
        ActionListener<Void> completionListener = ActionListener.wrap(resp -> {
            try {
                postUploadRunner.run();
                listener.onResponse(null);
            } catch (Exception e) {
                logger.error("Exception in segment postUpload for file {} format {}", src, dataFormat.name(), e);
                listener.onFailure(e);
            }
        }, ex -> {
            logger.error("Failed to upload blob {} format {}", src, dataFormat.name(), ex);
            listener.onFailure(ex);
        });
        
        completionListener = ActionListener.runBefore(completionListener, () -> {
            try {
                remoteTransferContainer.close();
            } catch (Exception e) {
                logger.warn("Error occurred while closing streams for file {} format {}", src, dataFormat.name(), e);
            }
        });
        
        WriteContext writeContext = remoteTransferContainer.createWriteContext();
        ((AsyncMultiStreamBlobContainer) blobContainer).asyncBlobUpload(writeContext, completionListener);
    }
    
    /**
     * Helper class to convert IndexInput to InputStream
     */
    private static class IndexInputInputStream extends InputStream {
        private final IndexInput indexInput;
        
        IndexInputInputStream(IndexInput indexInput) {
            this.indexInput = indexInput;
        }
        
        @Override
        public int read() throws IOException {
            if (indexInput.getFilePointer() >= indexInput.length()) {
                return -1;
            }
            return indexInput.readByte() & 0xFF;
        }
        
        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            long remaining = indexInput.length() - indexInput.getFilePointer();
            if (remaining <= 0) {
                return -1;
            }
            
            int toRead = (int) Math.min(len, remaining);
            indexInput.readBytes(b, off, toRead);
            return toRead;
        }
        
        @Override
        public void close() throws IOException {
            indexInput.close();
        }
    }
    
    @Override
    public String toString() {
        return "GenericRemoteDirectory{" +
               "format=" + dataFormat.name() +
               ", remoteBasePath='" + remoteBasePath + '\'' +
               ", acceptedExtensions=" + acceptedExtensions +
               '}';
    }
}

/**
 * IndexOutput implementation that writes to BlobContainer for generic remote directories.
 * This enables non-Lucene formats to use IndexOutput interface while writing to blob storage.
 */
class BlobContainerIndexOutput extends IndexOutput {
    private final String fileName;
    private final BlobContainer blobContainer;
    private final Logger logger;
    private final ByteArrayOutputStream buffer;
    
    BlobContainerIndexOutput(String fileName, BlobContainer blobContainer, Logger logger) {
        super("BlobContainerIndexOutput(" + fileName + ")", fileName);
        this.fileName = fileName;
        this.blobContainer = blobContainer;
        this.logger = logger;
        this.buffer = new ByteArrayOutputStream();
    }
    
    @Override
    public void writeByte(byte b) throws IOException {
        buffer.write(b & 0xFF);
    }
    
    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        buffer.write(b, offset, length);
    }
    
    @Override
    public long getFilePointer() {
        return buffer.size();
    }
    
    @Override
    public long getChecksum() throws IOException {
        // Generic formats may not need complex checksum validation
        // Return a simple checksum based on content
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(buffer.toByteArray());
        return crc.getValue();
    }
    
    @Override
    public void close() throws IOException {
        try {
            // Upload the buffered content to blob container
            byte[] content = buffer.toByteArray();
            blobContainer.writeBlob(fileName, new ByteArrayInputStream(content), content.length, false);
            logger.debug("Successfully uploaded {} bytes to blob container for file: {}", content.length, fileName);
        } finally {
            buffer.close();
        }
    }
}
