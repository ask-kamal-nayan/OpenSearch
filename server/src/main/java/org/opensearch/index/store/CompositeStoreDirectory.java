/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.coord.Any;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.DataSourcePlugin;
import org.opensearch.plugins.PluginsService;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Composite directory that coordinates multiple format-specific directories.
 * Routes file operations to appropriate format directories based on file type.
 * Implements both Directory and FormatStoreDirectory interfaces for compatibility.
 *
 * Follows the same plugin-based architecture pattern as CompositeIndexingExecutionEngine.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.0.0")
public class CompositeStoreDirectory extends Directory implements FormatStoreDirectory<Any> {

    private Any dataFormat;
    private final Path directoryPath;
    public final List<FormatStoreDirectory<?>> delegates = new ArrayList<>();
    private final Logger logger;
    private final DirectoryFileTransferTracker directoryFileTransferTracker;
    private final ShardPath shardPath;

    /**
     * Constructor following CompositeIndexingExecutionEngine pattern exactly
     */
    public CompositeStoreDirectory(IndexSettings indexSettings, PluginsService pluginsService, Any dataFormats, ShardPath shardPath, Logger logger) {
        this.dataFormat = dataFormats;
        this.shardPath = shardPath;
        this.logger = logger;
        this.directoryFileTransferTracker = new DirectoryFileTransferTracker();
        this.directoryPath = shardPath.getDataPath();
        try {
            for (DataFormat dataFormat : dataFormats.getDataFormats()) {
                DataSourcePlugin plugin = pluginsService.filterPlugins(DataSourcePlugin.class).stream()
                    .filter(curr -> curr.getDataFormat().equals(dataFormat.name()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("dataformat [" + dataFormat + "] is not registered."));
                delegates.add(plugin.createFormatStoreDirectory(indexSettings, shardPath));
            }
        } catch (NullPointerException e) {
            // Fallback - same as CompositeIndexingExecutionEngine
            try {
                delegates.add(new GenericStoreDirectory<>(DataFormat.TEXT, shardPath, logger));
            } catch (IOException ex) {
                throw new RuntimeException("Failed to create fallback directory", ex);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        logger.debug("Created CompositeStoreDirectory with {} format directories",
            delegates.size());
    }

    /**
     * Simplified constructor for auto-discovery (like CompositeIndexingExecutionEngine)
     */
    public CompositeStoreDirectory(IndexSettings indexSettings, PluginsService pluginsService, ShardPath shardPath, Logger logger) {
        this.shardPath = shardPath;
        this.logger = logger;
        this.directoryFileTransferTracker = new DirectoryFileTransferTracker();
        this.directoryPath = shardPath.getDataPath();

        try {
            DataSourcePlugin plugin = pluginsService.filterPlugins(DataSourcePlugin.class).stream()
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("dataformat [" + DataFormat.TEXT + "] is not registered."));
            delegates.add(plugin.createFormatStoreDirectory(indexSettings, shardPath));
        } catch (NullPointerException | IOException e) {
            try {
                delegates.add(new GenericStoreDirectory<>(DataFormat.TEXT, shardPath, logger));
            } catch (IOException ex) {
                throw new RuntimeException("Failed to create fallback directory", ex);
            }
        }
    }

    // ===== FormatStoreDirectory<Any> Implementation =====

    @Override
    public Any getDataFormat() {
        return dataFormat;
    }

    @Override
    public boolean acceptsFile(String fileName) {
        // CompositeStoreDirectory accepts any file that any delegate accepts
        return delegates.stream().anyMatch(delegate -> delegate.acceptsFile(fileName));
    }

    @Override
    public Path getDirectoryPath() {
        // Return the shard path as this is the composite root
        return shardPath.getDataPath();
    }

    @Override
    public void initialize() throws IOException {
        // Initialize all delegates
        for (FormatStoreDirectory<?> delegate : delegates) {
            delegate.initialize();
        }
    }

    @Override
    public void cleanup() throws IOException {
        // Cleanup all delegates
        for (FormatStoreDirectory<?> delegate : delegates) {
            delegate.cleanup();
        }
    }

    /**
     * Routes file operation to the appropriate format directory with comprehensive format detection logging.
     * Provides detailed monitoring information for format routing decisions and troubleshooting.
     */
    public FormatStoreDirectory getDirectoryForFile(String fileName) {
        logger.debug("Starting format detection for file: {}", fileName);

        // Track which formats were checked for detailed troubleshooting
        List<String> checkedFormats = new ArrayList<>();

        for (FormatStoreDirectory directory : delegates) {
            String formatName = directory.getDataFormat().name();
            checkedFormats.add(formatName);

            logger.trace("Checking format {} for file {}: acceptsFile={}",
                formatName, fileName, directory.acceptsFile(fileName));

            if (directory.acceptsFile(fileName)) {
                logger.debug("Format detection successful: file={}, selectedFormat={}, checkedFormats={}, directoryPath={}",
                    fileName, formatName, checkedFormats, directory.getDirectoryPath());
                return directory;
            }
        }

        // Enhanced error logging for format detection failures
        logger.error("Format detection failed: file={}, checkedFormats={}, availableFormats={}",
            fileName, checkedFormats, delegates.stream().map(d -> d.getDataFormat().name()).toList());

        // If no format accepts the file, throw an exception to avoid format bias
//        throw new IllegalArgumentException("No format directory accepts file: " + fileName +
//            ". Checked formats: " + checkedFormats +
//            ". Available formats: " + delegates.stream().map(d -> d.getDataFormat().name()).toList());
        return delegates.get(0);
    }

    /**
     * Returns all format directories for operations that need to span all formats
     */
    @SuppressWarnings("unchecked")
    public Collection<FormatStoreDirectory> getAllDirectories() {
        return (Collection<FormatStoreDirectory>) (Collection<?>) delegates;
    }

    /**
     * Returns directory for specific format
     */
    public FormatStoreDirectory getDirectoryForFormat(DataFormat format) {
        return delegates.stream()
            .filter(delegate -> delegate.getDataFormat().equals(format))
            .findFirst()
            .orElse(null);
    }

    // ===== FormatStoreDirectory<Any> Required Methods =====

    @Override
    public OutputStream createOutput(String name) throws IOException {
        FormatStoreDirectory directory = getDirectoryForFile(name);
        return directory.createOutput(name);
    }

    @Override
    public InputStream openInput(String name) throws IOException {
        FormatStoreDirectory directory = getDirectoryForFile(name);
        return directory.openInput(name);
    }

    @Override
    public boolean fileExists(String name) throws IOException {
        FormatStoreDirectory directory = getDirectoryForFile(name);
        return directory.fileExists(name);
    }

    @Override
    public String calculateUploadChecksum(String fileName) throws IOException {
        FormatStoreDirectory directory = getDirectoryForFile(fileName);
        return directory.calculateUploadChecksum(fileName);
    }

    @Override
    public InputStream createUploadInputStream(String fileName) throws IOException {
        FormatStoreDirectory directory = getDirectoryForFile(fileName);
        return directory.createUploadInputStream(fileName);
    }

    @Override
    public InputStream createUploadRangeInputStream(String fileName, long offset, long length) throws IOException {
        FormatStoreDirectory directory = getDirectoryForFile(fileName);
        return directory.createUploadRangeInputStream(fileName, offset, length);
    }

    @Override
    public void onUploadComplete(String fileName, String remoteFileName) throws IOException {
        FormatStoreDirectory directory = getDirectoryForFile(fileName);
        directory.onUploadComplete(fileName, remoteFileName);
    }

    @Override
    public IndexInput openIndexInput(String name, IOContext context) throws IOException {
        FormatStoreDirectory directory = getDirectoryForFile(name);
        return directory.openIndexInput(name, context);
    }

    // Directory interface implementation with routing
    @Override
    public String[] listAll() throws IOException {
        Set<String> allFiles = new HashSet<>();
        for (FormatStoreDirectory directory : delegates) {
            allFiles.addAll(Arrays.asList(directory.listAll()));
        }
        return allFiles.toArray(new String[0]);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        getDirectoryForFile(name).deleteFile(name);
    }

    @Override
    public long fileLength(String name) throws IOException {
        return getDirectoryForFile(name).fileLength(name);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        FormatStoreDirectory directory = getDirectoryForFile(name);

        // For Lucene directory, delegate directly to the wrapped Directory for better performance
        if (directory instanceof LuceneStoreDirectory) {
            LuceneStoreDirectory luceneDir = (LuceneStoreDirectory) directory;
            return luceneDir.getWrappedDirectory().createOutput(name, context);
        }

        // For generic directories, adapt OutputStream to IndexOutput
        OutputStream outputStream = directory.createOutput(name);
        return new OutputStreamIndexOutput(outputStream, name);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        // For temporary files, use the first available directory that supports temp file creation
        // Try Lucene directory first for backward compatibility, but don't require it
        FormatStoreDirectory luceneDirectory = getDirectoryForFormat(DataFormat.LUCENE);
        if (luceneDirectory instanceof LuceneStoreDirectory) {
            LuceneStoreDirectory luceneDir = (LuceneStoreDirectory) luceneDirectory;
            return luceneDir.getWrappedDirectory().createTempOutput(prefix, suffix, context);
        }

        // If no Lucene directory, use the first available directory
        FormatStoreDirectory firstDirectory = delegates.get(0);
        if (firstDirectory instanceof LuceneStoreDirectory) {
            LuceneStoreDirectory luceneDir = (LuceneStoreDirectory) firstDirectory;
            return luceneDir.getWrappedDirectory().createTempOutput(prefix, suffix, context);
        }

        // For generic directories, create a temporary file name and delegate to createOutput
        String tempFileName = prefix + System.currentTimeMillis() + suffix;
        OutputStream outputStream = firstDirectory.createOutput(tempFileName);
        return new OutputStreamIndexOutput(outputStream, tempFileName);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        FormatStoreDirectory directory = getDirectoryForFile(name);

        // For Lucene directory, delegate directly to the wrapped Directory for better performance
        if (directory instanceof LuceneStoreDirectory) {
            LuceneStoreDirectory luceneDir = (LuceneStoreDirectory) directory;
            return luceneDir.getWrappedDirectory().openInput(name, context);
        }

        // For generic directories, adapt InputStream to IndexInput
        InputStream inputStream = directory.openInput(name);
        Path filePath = directory.getDirectoryPath().resolve(name);
        return InputStreamIndexInput.create(inputStream, name, filePath);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        // Group files by directory and sync each directory
        Map<FormatStoreDirectory, List<String>> filesByDirectory = new HashMap<>();

        for (String name : names) {
            FormatStoreDirectory directory = getDirectoryForFile(name);
            filesByDirectory.computeIfAbsent(directory, k -> new ArrayList<>()).add(name);
        }

        for (Map.Entry<FormatStoreDirectory, List<String>> entry : filesByDirectory.entrySet()) {
            entry.getKey().sync(entry.getValue());
        }
    }

    @Override
    public void syncMetaData() throws IOException {
        // Sync metadata for all directories
        for (FormatStoreDirectory directory : delegates) {
            directory.syncMetaData();
        }
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        FormatStoreDirectory sourceDir = getDirectoryForFile(source);
        FormatStoreDirectory destDir = getDirectoryForFile(dest);

        if (sourceDir != destDir) {
            throw new IOException("Cannot rename file across different format directories: " +
                                source + " (" + sourceDir.getDataFormat().name() + ") -> " +
                                dest + " (" + destDir.getDataFormat().name() + ")");
        }

        sourceDir.rename(source, dest);
    }

    @Override
    public Lock obtainLock(String name) throws IOException {
        // For lock files, try to route to appropriate directory or use first available
        FormatStoreDirectory directory;
        try {
            directory = getDirectoryForFile(name);
        } catch (IllegalArgumentException e) {
            // If no format accepts the lock file, use the first available directory
            directory = delegates.get(0);
            logger.debug("No format accepts lock file {}, using first available directory: {}",
                name, directory.getDataFormat().name());
        }

        // For Lucene directory, delegate to the wrapped Directory for proper lock handling
        if (directory instanceof LuceneStoreDirectory) {
            LuceneStoreDirectory luceneDir = (LuceneStoreDirectory) directory;
            return luceneDir.getWrappedDirectory().obtainLock(name);
        }

        // For generic directories, create a file-based lock
        GenericFileLock lock = new GenericFileLock(directory.getDirectoryPath().resolve(name + ".lock"));
        lock.acquire(); // Actually acquire the lock
        return lock;
    }

    @Override
    public void close() throws IOException {
        IOException firstException = null;

        for (FormatStoreDirectory directory : delegates) {
            try {
                directory.close();
            } catch (IOException e) {
                if (firstException == null) {
                    firstException = e;
                } else {
                    firstException.addSuppressed(e);
                }
            }
        }

        if (firstException != null) {
            throw firstException;
        }
    }

    /**
     * Estimate the cumulative size of all files across all format directories
     */
    public long estimateSize() throws IOException {
        long totalSize = 0;
        for (FormatStoreDirectory directory : delegates) {
            String[] files = directory.listAll();
            for (String file : files) {
                try {
                    totalSize += directory.fileLength(file);
                } catch (IOException e) {
                    logger.debug("Failed to get file length for {}: {}", file, e.getMessage());
                    // Continue with other files
                }
            }
        }
        return totalSize;
    }

    /**
     * Calculate checksum by routing to appropriate format directory
     * @param fileName the name of the file to calculate checksum for
     * @return the checksum as a string representation
     * @throws IOException if checksum calculation fails
     */
    public long calculateChecksum(String fileName) throws IOException {
        FormatStoreDirectory directory = getDirectoryForFile(fileName);

        logger.debug("Calculating checksum for file {} using format directory: {}",
            fileName, directory.getDataFormat().name());

        try {
            long checksum = directory.calculateChecksum(fileName);
            logger.trace("Successfully calculated checksum for file {}: {}", fileName, checksum);
            return checksum;
        } catch (IOException e) {
            logger.error("Failed to calculate checksum for file {} using format {}: {}",
                fileName, directory.getDataFormat().name(), e.getMessage(), e);
            throw new IOException(
                String.format(
                    "Failed to calculate checksum for file %s using format %s at path %s: %s",
                    fileName,
                    directory.getDataFormat().name(),
                    directory.getDirectoryPath().resolve(fileName),
                    e.getMessage()
                ),
                e
            );
        } catch (Exception e) {
            logger.error("Unexpected error calculating checksum for file {} using format {}: {}",
                fileName, directory.getDataFormat().name(), e.getMessage(), e);
            throw new IOException("Unexpected error calculating checksum for file: " + fileName, e);
        }
    }

    /**
     * Get checksum of local file - delegates to calculateChecksum for consistency.
     * This method is used by RemoteSegmentStoreDirectory for format-agnostic uploads.
     * @param fileName the name of the file to get checksum for
     * @return the checksum as a string representation
     * @throws IOException if checksum calculation fails
     */
    public long getChecksumOfLocalFile(String fileName) throws IOException {
        logger.debug("Getting checksum of local file: {}", fileName);
        return calculateChecksum(fileName);
    }

    /**
     * Get the directory file transfer tracker for this composite directory
     */
    public DirectoryFileTransferTracker getDirectoryFileTransferTracker() {
        return directoryFileTransferTracker;
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        // Aggregate pending deletions from all format directories
        // For now, return empty set as FormatStoreDirectory doesn't track pending deletions
        return Set.of();
    }
}

/**
 * Adapter class that converts OutputStream to Lucene IndexOutput
 */
class OutputStreamIndexOutput extends IndexOutput {
    private final OutputStream outputStream;
    private final String name;
    private long bytesWritten = 0;

    OutputStreamIndexOutput(OutputStream outputStream, String name) {
        super("OutputStreamIndexOutput(" + name + ")", name);
        this.outputStream = outputStream;
        this.name = name;
    }

    @Override
    public void writeByte(byte b) throws IOException {
        outputStream.write(b & 0xFF);
        bytesWritten++;
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        outputStream.write(b, offset, length);
        bytesWritten += length;
    }

    @Override
    public long getFilePointer() {
        return bytesWritten;
    }

    @Override
    public long getChecksum() throws IOException {
        // Generic implementation - checksum not supported
        throw new UnsupportedOperationException("Checksum not supported for generic output streams");
    }

    @Override
    public void close() throws IOException {
        outputStream.close();
    }
}

/**
 * Adapter class that converts InputStream to Lucene IndexInput
 * Note: This implementation requires the InputStream to be created from a file path
 * so that we can determine the file length.
 */
class InputStreamIndexInput extends IndexInput {
    private final InputStream inputStream;
    private final String name;
    private final long length;
    private long position = 0;

    /**
     * Creates an IndexInput adapter for the given InputStream and file path.
     * The filePath is used to determine the file length.
     */
    static InputStreamIndexInput create(InputStream inputStream, String name, Path filePath) throws IOException {
        long fileLength = Files.size(filePath);
        return new InputStreamIndexInput(inputStream, name, fileLength);
    }

    private InputStreamIndexInput(InputStream inputStream, String name, long length) throws IOException {
        super("InputStreamIndexInput(" + name + ")");
        this.inputStream = inputStream;
        this.name = name;
        this.length = length;
    }

    @Override
    public byte readByte() throws IOException {
        if (position >= length) {
            throw new IOException("Read past EOF: " + name);
        }
        int b = inputStream.read();
        if (b == -1) {
            throw new IOException("Unexpected EOF: " + name);
        }
        position++;
        return (byte) b;
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        if (position + len > length) {
            throw new IOException("Read past EOF: " + name);
        }

        int totalRead = 0;
        while (totalRead < len) {
            int read = inputStream.read(b, offset + totalRead, len - totalRead);
            if (read == -1) {
                throw new IOException("Unexpected EOF: " + name);
            }
            totalRead += read;
        }
        position += len;
    }

    @Override
    public long getFilePointer() {
        return position;
    }

    @Override
    public void seek(long pos) throws IOException {
        throw new UnsupportedOperationException("Seek not supported for generic input streams");
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        throw new UnsupportedOperationException("Slice not supported for generic input streams");
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
    }
}

/**
 * Generic file lock implementation using Java NIO file locking
 */
class GenericFileLock extends Lock {
    private final Path lockFile;
    private FileChannel channel;
    private FileLock fileLock;

    GenericFileLock(Path lockFile) {
        this.lockFile = lockFile;
    }

    @Override
    public void ensureValid() throws IOException {
        if (fileLock == null || !fileLock.isValid()) {
            throw new IOException("Lock is not valid: " + lockFile);
        }
    }

    @Override
    public void close() throws IOException {
        if (fileLock != null) {
            try {
                fileLock.release();
            } finally {
                fileLock = null;
            }
        }

        if (channel != null) {
            try {
                channel.close();
            } finally {
                channel = null;
            }
        }

        // Clean up lock file
        Files.deleteIfExists(lockFile);
    }

    /**
     * Acquire the lock
     */
    void acquire() throws IOException {
        // Create parent directories if needed
        Files.createDirectories(lockFile.getParent());

        // Open channel and acquire lock
        channel = FileChannel.open(lockFile,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING);

        fileLock = channel.tryLock();
        if (fileLock == null) {
            channel.close();
            throw new IOException("Could not acquire lock: " + lockFile);
        }
    }
}
