/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.ExceptionsHelper;
import org.opensearch.common.UUIDs;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.exception.CorruptFileException;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.blobstore.transfer.RemoteTransferContainer;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeIndexInputStream;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeInputStream;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.coord.Any;
import org.opensearch.index.store.CompositeStoreDirectory;
import org.opensearch.index.store.FormatStoreDirectory;
import org.opensearch.index.store.RemoteDirectory;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;

/**
 * CompositeRemoteDirectory with direct BlobContainer access per format.
 *
 * Key Architecture:
 * - Map of DataFormat to BlobContainer for direct blob access
 * - Lazy BlobContainer creation when new formats are encountered
 * - Format determination from file path/local directory routing
 * - ALL formats get equal treatment with same generic streaming logic
 *
 * @opensearch.api
 */
@PublicApi(since = "3.0.0")
public class CompositeRemoteDirectory implements Closeable {

    private Any dataFormat;

    private final UnaryOperator<OffsetRangeInputStream> uploadRateLimiter;

    private final UnaryOperator<OffsetRangeInputStream> lowPriorityUploadRateLimiter;

    private final DownloadRateLimiterProvider downloadRateLimiterProvider;

    /**
     * Map containing the mapping of segment files that are pending download as part of the pre-copy (warm) phase of
     * {@link org.opensearch.index.engine.MergedSegmentWarmer}. The key is the local filename and value is the remote filename.
     */
    final Map<String, String> pendingDownloadMergedSegments;

    /**
     * Separator for generating unique remote segment filenames
     */
    public static final String SEGMENT_NAME_UUID_SEPARATOR = "__";

    // Core architecture: Direct BlobContainer per format with lazy creation
    private final Map<String, BlobContainer> formatBlobContainers;
    private final BlobStore blobStore;
    private final BlobPath baseBlobPath;
    private final Logger logger;

    /**
     * Full constructor with all rate limiter parameters
     */
    public CompositeRemoteDirectory(
        BlobStore blobStore,
        BlobPath baseBlobPath,
        UnaryOperator<OffsetRangeInputStream> uploadRateLimiter,
        UnaryOperator<OffsetRangeInputStream> lowPriorityUploadRateLimiter,
        UnaryOperator<InputStream> downloadRateLimiter,
        UnaryOperator<InputStream> lowPriorityDownloadRateLimiter,
        Map<String, String> pendingDownloadMergedSegments,
        Logger logger
    ) {
        this.formatBlobContainers = new ConcurrentHashMap<>();
        this.blobStore = blobStore;
        this.baseBlobPath = baseBlobPath;
        this.uploadRateLimiter = uploadRateLimiter;
        this.lowPriorityUploadRateLimiter = lowPriorityUploadRateLimiter;
        this.downloadRateLimiterProvider = new DownloadRateLimiterProvider(downloadRateLimiter, lowPriorityDownloadRateLimiter);
        this.pendingDownloadMergedSegments = pendingDownloadMergedSegments;
        this.logger = logger;

        logger.debug("Created CompositeRemoteDirectory with lazy BlobContainer creation at base path: {}",
            baseBlobPath);
    }

    /**
     * Creates CompositeRemoteDirectory with lazy BlobContainer creation per format.
     *
     * @param blobStore the underlying blob store for creating format-specific containers
     * @param baseBlobPath base path for all format containers
     * @param logger logger for this composite directory
     */
    public CompositeRemoteDirectory(
        BlobStore blobStore,
        BlobPath baseBlobPath,
        Logger logger
    ) {
        this(blobStore, baseBlobPath, UnaryOperator.identity(), UnaryOperator.identity(), UnaryOperator.identity(), UnaryOperator.identity(), null, logger);
    }

    /**
     * Constructor following RemoteDirectory pattern for multiple formats
     * Creates BlobContainers for each format - similar to RemoteDirectory but with multiple containers
     */
    public CompositeRemoteDirectory(
        BlobStore blobStore,
        BlobPath baseBlobPath,
        Any dataFormats,
        Logger logger
    ) {
        this(blobStore, baseBlobPath, UnaryOperator.identity(), UnaryOperator.identity(), UnaryOperator.identity(), UnaryOperator.identity(), null, logger);
        
        this.dataFormat = dataFormats;

        // Eagerly create BlobContainers for each format
        for (DataFormat format : dataFormats.getDataFormats()) {
            // Create format-specific BlobPath: basePath/formatName/
            BlobPath formatPath = baseBlobPath.add(format.name().toLowerCase());
            BlobContainer container = blobStore.blobContainer(formatPath);
            formatBlobContainers.put(format.name(), container);

            logger.debug("Created BlobContainer for format {} at path: {}", format.name(), formatPath);
        }

        logger.debug("Created CompositeRemoteDirectory with {} format BlobContainers",
            formatBlobContainers.size());
    }

    /**
     * Get or create BlobContainer for specific format.
     * This is where the lazy creation happens - if we don't have a BlobContainer
     * for this format yet, we create one and store it in the map.
     *
     * @param format the data format
     * @return BlobContainer for the format (created if not exists)
     */
    public BlobContainer getBlobContainerForFormat(String format) {
        return formatBlobContainers.computeIfAbsent(format, f -> {
            // Create format-specific BlobPath: basePath/formatName/
            BlobPath formatPath = baseBlobPath.add(f.toLowerCase());
            BlobContainer container = blobStore.blobContainer(formatPath);

            logger.debug("Created new BlobContainer for format {} at path: {}", f, formatPath);
            return container;
        });
    }

    /**
     * Determine format from file path by routing through CompositeStoreDirectory.
     * This uses the same file extension/pattern logic as the local directories.
     *
     * @param from CompositeStoreDirectory source
     * @param fileName file name to determine format for
     * @return DataFormat for the file
     */
    private String determineFormatFromFile(CompositeStoreDirectory from, String fileName) {
        try {
            // Route through CompositeStoreDirectory to determine which format accepts this file
            FormatStoreDirectory formatDir = from.getDirectoryForFile(fileName);
            return formatDir.getDataFormat().name();
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot determine format for file: " + fileName, e);
        }
    }

    /**
     * Format-agnostic copyFrom with lazy BlobContainer creation.
     *
     * Flow:
     * 1. Determine format from file path via CompositeStoreDirectory routing (SINGLE detection point)
     * 2. Get or create BlobContainer for that format (lazy creation)
     * 3. Upload file directly to format's BlobContainer using generic streaming
     * 4. ALL formats use identical logic - no special cases
     *
     * @param from CompositeStoreDirectory source
     * @param src source file name
     * @param context IO context
     * @param listener completion listener
     * @param lowPriorityUpload whether to use low priority upload
     */
    public boolean copyFrom(
        CompositeStoreDirectory from,
        String src,
        String remoteFileName,
        IOContext context,
        Runnable postUploadRunner,
        ActionListener<Void> listener,
        boolean lowPriorityUpload
    ) {
        BlobContainer blobContainer = getBlobContainerForFormat(determineFormatFromFile(from, remoteFileName));
        if (blobContainer instanceof AsyncMultiStreamBlobContainer) {
            try {
                uploadBlob(from, src, remoteFileName, context, postUploadRunner, listener, lowPriorityUpload);
            } catch (Exception e) {
                listener.onFailure(e);
            }
            return true;
        }
        return false;
    }

//    /**
//     * Generic streaming copy that works identically for ALL formats.
//     * Lucene, Parquet, Custom formats - all use exactly the same logic.
//     */
//    private void performGenericStreamingCopy(CompositeRemoteDirectory sourceDirectory, String src,
//                                           BlobContainer targetBlobContainer, DataFormat format,
//                                           IOContext context, ActionListener<Void> listener,
//                                           boolean lowPriorityUpload) throws IOException {
//
//        // Verify source file exists
//        if (!sourceDirectory.fileExists(src)) {
//            throw new FileNotFoundException("Source file not found: " + src + " in format: " + format.name());
//        }
//
//        // Generate unique remote filename
//        String remoteFileName = generateRemoteFileName(src);
//        long fileLength = sourceDirectory.fileLength(src);
//
//        logger.trace("Starting generic stream copy for file {} (format: {}, length: {})",
//            src, format.name(), fileLength);
//
//        // Generic streaming upload - identical logic for all formats
//        try (InputStream inputStream = sourceDirectory.openInput(src)) {
//            // Calculate checksum while streaming (format-agnostic)
//            ChecksumCalculatingInputStream checksumStream = new ChecksumCalculatingInputStream(inputStream);
//
//            // Direct blob upload to format's container - same for all formats
//            performGenericBlobUpload(checksumStream, targetBlobContainer, remoteFileName, fileLength,
//                src, format, listener, lowPriorityUpload);
//
//        } catch (IOException e) {
//            throw new IOException("Generic streaming copy failed for file: " + src + " format: " + format.name(), e);
//        }
//    }

    private void uploadBlob(
        CompositeStoreDirectory from,
        String src,
        String remoteFileName,
        IOContext ioContext,
        Runnable postUploadRunner,
        ActionListener<Void> listener,
        boolean lowPriorityUpload
    ) throws Exception {
        assert ioContext != IOContext.READONCE : "Remote upload will fail with IoContext.READONCE";
        long expectedChecksum = calculateChecksumOfChecksum(from, src);
        long contentLength;
        IndexInput indexInput = from.openInput(src, ioContext);
        try {
            contentLength = indexInput.length();
            BlobContainer targetBlobContainer = getBlobContainer(from, src);
            boolean remoteIntegrityEnabled = false;
            if (targetBlobContainer instanceof AsyncMultiStreamBlobContainer) {
                remoteIntegrityEnabled = ((AsyncMultiStreamBlobContainer) targetBlobContainer).remoteIntegrityCheckSupported();
            }
            lowPriorityUpload = lowPriorityUpload || contentLength > ByteSizeUnit.GB.toBytes(15);
            RemoteTransferContainer.OffsetRangeInputStreamSupplier offsetRangeInputStreamSupplier;

            if (lowPriorityUpload) {
                offsetRangeInputStreamSupplier = (size, position) -> lowPriorityUploadRateLimiter.apply(
                    new OffsetRangeIndexInputStream(indexInput.clone(), size, position)
                );
            } else {
                offsetRangeInputStreamSupplier = (size, position) -> uploadRateLimiter.apply(
                    new OffsetRangeIndexInputStream(indexInput.clone(), size, position)
                );
            }
            RemoteTransferContainer remoteTransferContainer = new RemoteTransferContainer(
                src,
                remoteFileName,
                contentLength,
                true,
                lowPriorityUpload ? WritePriority.LOW : WritePriority.NORMAL,
                offsetRangeInputStreamSupplier,
                expectedChecksum,
                remoteIntegrityEnabled
            );
            ActionListener<Void> completionListener = ActionListener.wrap(resp -> {
                try {
                    postUploadRunner.run();
                    listener.onResponse(null);
                } catch (Exception e) {
                    logger.error(() -> new ParameterizedMessage("Exception in segment postUpload for file [{}]", src), e);
                    listener.onFailure(e);
                }
            }, ex -> {
                logger.error(() -> new ParameterizedMessage("Failed to upload blob {}", src), ex);
                IOException corruptIndexException = ExceptionsHelper.unwrapCorruption(ex);
                if (corruptIndexException != null) {
                    listener.onFailure(corruptIndexException);
                    return;
                }
                Throwable throwable = ExceptionsHelper.unwrap(ex, CorruptFileException.class);
                if (throwable != null) {
                    CorruptFileException corruptFileException = (CorruptFileException) throwable;
                    listener.onFailure(new CorruptIndexException(corruptFileException.getMessage(), corruptFileException.getFileName()));
                    return;
                }
                listener.onFailure(ex);
            });

            completionListener = ActionListener.runBefore(completionListener, () -> {
                try {
                    remoteTransferContainer.close();
                } catch (Exception e) {
                    logger.warn("Error occurred while closing streams", e);
                }
            });

            completionListener = ActionListener.runAfter(completionListener, () -> {
                try {
                    indexInput.close();
                } catch (IOException e) {
                    logger.warn("Error occurred while closing index input", e);
                }
            });

            WriteContext writeContext = remoteTransferContainer.createWriteContext();
            ((AsyncMultiStreamBlobContainer) targetBlobContainer).asyncBlobUpload(writeContext, completionListener);
        } catch (Exception e) {
            logger.warn("Exception while calling asyncBlobUpload, closing IndexInput to avoid leak");
            indexInput.close();
            throw e;
        }
    }

    private long calculateChecksumOfChecksum(CompositeStoreDirectory from, String src) throws IOException {
        return from.calculateChecksum(src);
    }

    /**
     * Generate unique remote filename - same pattern for all formats
     */
    private String generateRemoteFileName(String localFileName) {
        return localFileName + SEGMENT_NAME_UUID_SEPARATOR + UUIDs.base64UUID();
    }

    /**
     * Generic post-upload handling - same for all formats
     */
    private void handleSuccessfulUpload(String originalFileName, String remoteFileName,
                                      String checksum, long fileLength, DataFormat format) {
        logger.debug("Successfully uploaded file {} as {} for format {} (checksum: {}, length: {})",
            originalFileName, remoteFileName, format.name(), checksum, fileLength);
    }

    /**
     * Get current format containers (for debugging/monitoring)
     */
    public Map<String, BlobContainer> getFormatBlobContainers() {
        return Map.copyOf(formatBlobContainers);
    }

    @Override
    public void close() throws IOException {
        logger.debug("Closed CompositeRemoteDirectory with {} format containers", formatBlobContainers.size());
        formatBlobContainers.clear();
    }

    @Override
    public String toString() {
        return "CompositeRemoteDirectory{" +
               "formats=" + formatBlobContainers.keySet() +
               ", basePath=" + baseBlobPath +
               '}';
    }

    private boolean isMergedSegment(String remoteFilename) {
        return pendingDownloadMergedSegments != null && pendingDownloadMergedSegments.containsValue(remoteFilename);
    }

    /**
     * Input stream wrapper that calculates checksum while reading
     */
    private static class ChecksumCalculatingInputStream extends InputStream {
        private final InputStream delegate;
        private final MessageDigest digest;
        private String checksum;

        ChecksumCalculatingInputStream(InputStream delegate) throws IOException {
            this.delegate = delegate;
            try {
                this.digest = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
                throw new IOException("SHA-256 algorithm not available", e);
            }
        }

        @Override
        public int read() throws IOException {
            int b = delegate.read();
            if (b != -1) {
                digest.update((byte) b);
            } else if (checksum == null) {
                checksum = bytesToHex(digest.digest());
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int bytesRead = delegate.read(b, off, len);
            if (bytesRead > 0) {
                digest.update(b, off, bytesRead);
            } else if (bytesRead == -1 && checksum == null) {
                checksum = bytesToHex(digest.digest());
            }
            return bytesRead;
        }

        @Override
        public void close() throws IOException {
            delegate.close();
            if (checksum == null) {
                checksum = bytesToHex(digest.digest());
            }
        }

        String getChecksum() {
            return checksum != null ? checksum : bytesToHex(digest.digest());
        }

        private static String bytesToHex(byte[] bytes) {
            StringBuilder result = new StringBuilder();
            for (byte b : bytes) {
                result.append(String.format("%02x", b));
            }
            return result.toString();
        }
    }

    BlobContainer getBlobContainer(CompositeStoreDirectory from, String src) throws IOException {
        return getBlobContainerForFormat(determineFormatFromFile(from, src));
    }

    /**
     * DownloadRateLimiterProvider returns a low-priority rate limited stream if the segment
     * being downloaded is a merged segment as part of the pre-copy (warm) phase of
     * {@link org.opensearch.index.engine.MergedSegmentWarmer}.
     */
    private class DownloadRateLimiterProvider {
        private final UnaryOperator<InputStream> downloadRateLimiter;
        private final UnaryOperator<InputStream> lowPriorityDownloadRateLimiter;

        DownloadRateLimiterProvider(
            UnaryOperator<InputStream> downloadRateLimiter,
            UnaryOperator<InputStream> lowPriorityDownloadRateLimiter
        ) {
            this.downloadRateLimiter = downloadRateLimiter;
            this.lowPriorityDownloadRateLimiter = lowPriorityDownloadRateLimiter;
        }

        public UnaryOperator<InputStream> get(final String filename) {
            if (isMergedSegment(filename)) {
                return lowPriorityDownloadRateLimiter;
            }
            return downloadRateLimiter;
        }
    }
}
