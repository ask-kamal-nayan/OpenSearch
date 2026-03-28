/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.checksum.ChecksumHandlerRegistry;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Set;

/**
 * Format-aware composite directory that extends {@link SubdirectoryAwareDirectory}.
 *
 * <p>This directory adds data format awareness on top of the subdirectory path routing
 * inherited from {@link SubdirectoryAwareDirectory}. It understands that files in different
 * subdirectories may belong to different data formats (Lucene, Parquet, Arrow, etc.) and
 * provides format-specific operations, most notably checksum calculation.</p>
 *
 * <p><strong>Inherited from SubdirectoryAwareDirectory:</strong></p>
 * <ul>
 *   <li>Path routing: plain filenames → index/, prefixed filenames → subdirectories</li>
 *   <li>File operations: openInput, createOutput, deleteFile, fileLength, listAll, rename, sync</li>
 * </ul>
 *
 * <p><strong>Added by CompositeStoreDirectory:</strong></p>
 * <ul>
 *   <li>FileMetadata support: parse file identifier strings into FileMetadata objects</li>
 *   <li>Format-aware checksum: Lucene files → CodecUtil footer, others → CRC32 full-file</li>
 *   <li>Dual API: callers can use String or FileMetadata for all operations</li>
 * </ul>
 *
 * <p><strong>File naming convention:</strong></p>
 * <pre>
 *   Lucene:   "_0.cfs"                → stored in &lt;shard&gt;/index/_0.cfs
 *   Parquet:  "parquet/_0_1.parquet"  → stored in &lt;shard&gt;/parquet/_0_1.parquet
 *   Arrow:    "arrow/_0_1.arrow"      → stored in &lt;shard&gt;/arrow/_0_1.arrow
 * </pre>
 *
 * <p><strong>Checksum strategy:</strong></p>
 * <ul>
 *   <li>Lucene/index files: {@code CodecUtil.retrieveChecksum()} — reads checksum from codec footer (fast, O(1))</li>
 *   <li>Non-Lucene files: Full-file CRC32 scan — computes CRC32 over all bytes (generic, O(n))</li>
 * </ul>
 *
 * @opensearch.api
 */
@PublicApi(since = "3.0.0")
public class CompositeStoreDirectory extends SubdirectoryAwareDirectory {

    private static final Logger logger = LogManager.getLogger(CompositeStoreDirectory.class);

    /**
     * The default format name for files without a prefix (Lucene segment files).
     * Files like "_0.cfs", "segments_1" are treated as belonging to the "lucene" format.
     */
    private static final String DEFAULT_FORMAT = "lucene";

    /**
     * Format names that map to the standard index/ directory (no subdirectory prefix).
     * Both "lucene" and "metadata" files live in the index directory.
     */
    private static final Set<String> INDEX_DIRECTORY_FORMATS = Set.of("lucene", "LUCENE", "metadata");

    /**
     * Set of registered non-Lucene format names (e.g., "parquet", "arrow").
     */
    private final Set<String> registeredFormats;

    /**
     * Registry that maps format names to their checksum handlers.
     * Used for format-specific checksum calculation via the Strategy pattern.
     */
    private final ChecksumHandlerRegistry checksumRegistry;

    /**
     * Constructs a CompositeStoreDirectory with a default ChecksumHandlerRegistry.
     *
     * <p>This constructor is provided for backward compatibility. It creates a default
     * registry with only the built-in Lucene handler. Non-Lucene formats will use the
     * generic CRC32 fallback.</p>
     *
     * @param delegate          the underlying FSDirectory (typically for &lt;shard&gt;/index/)
     * @param shardPath         the shard path for resolving subdirectories
     * @param registeredFormats set of non-Lucene format names discovered from plugins
     *                          (e.g., {"parquet", "arrow"}).
     */
    public CompositeStoreDirectory(Directory delegate, ShardPath shardPath, Set<String> registeredFormats) {
        this(delegate, shardPath, registeredFormats, new ChecksumHandlerRegistry());
    }

    /**
     * Constructs a CompositeStoreDirectory with a custom ChecksumHandlerRegistry.
     *
     * <p>This is the preferred constructor. The registry is built by
     * {@code DefaultCompositeStoreDirectoryFactory} from plugin-provided checksum handlers.</p>
     *
     * @param delegate          the underlying FSDirectory (typically for &lt;shard&gt;/index/)
     * @param shardPath         the shard path for resolving subdirectories
     * @param registeredFormats set of non-Lucene format names discovered from plugins
     *                          (e.g., {"parquet", "arrow"}).
     * @param checksumRegistry  registry of format-specific checksum handlers
     */
    public CompositeStoreDirectory(Directory delegate, ShardPath shardPath, Set<String> registeredFormats,
                                   ChecksumHandlerRegistry checksumRegistry) {
        super(delegate, shardPath);
        this.registeredFormats = registeredFormats;
        this.checksumRegistry = checksumRegistry;

        // Pre-create format subdirectories so that native writers (e.g., Rust Parquet writer)
        // can access them directly without going through Java's Directory API.
        for (String format : registeredFormats) {
            try {
                Files.createDirectories(shardPath.getDataPath().resolve(format));
            } catch (IOException e) {
                logger.warn("Failed to pre-create directory for format: {}", format, e);
            }
        }

        logger.debug("Created CompositeStoreDirectory for shard {} with registered formats: {}, checksum handlers: {}",
            shardPath.getShardId(), registeredFormats, checksumRegistry.getRegisteredFormats());
    }

    // ═══════════════════════════════════════════════════════════════
    // Backward-compatible path resolution
    // Handles both new "format/file" and old "file:::format" conventions
    // ═══════════════════════════════════════════════════════════════

    /**
     * Overrides path resolution to handle backward compatibility with the old
     * {@code "file:::format"} FileMetadata serialization format.
     *
     * <p>If the filename contains the {@code ":::"} delimiter, it is first normalized
     * to the new path-based format before resolving the filesystem path.</p>
     *
     * @param fileName the file identifier (may use either convention)
     * @return the absolute filesystem path
     */
    @Override
    protected String parseFilePath(String fileName) {
        // Backward compatibility: convert old "file:::format" to new "format/file" convention
        if (fileName.contains(FileMetadata.DELIMITER)) {
            FileMetadata fm = new FileMetadata(fileName);  // uses existing ::: parsing logic
            fileName = toFileIdentifier(fm);  // converts to "format/file" or just "file" for lucene
        }
        return super.parseFilePath(fileName);
    }

    // ═══════════════════════════════════════════════════════════════
    // FileMetadata Parsing — bidirectional conversion between
    // path-style strings and structured FileMetadata objects
    // ═══════════════════════════════════════════════════════════════

    /**
     * Parses a file identifier string into a {@link FileMetadata} object.
     *
     * <p>The format is determined by the path prefix:</p>
     * <ul>
     *   <li>{@code "parquet/_0_1.parquet"} → {@code FileMetadata("parquet", "_0_1.parquet")}</li>
     *   <li>{@code "_0.cfs"} → {@code FileMetadata("index", "_0.cfs")}</li>
     *   <li>{@code "segments_1"} → {@code FileMetadata("index", "segments_1")}</li>
     * </ul>
     *
     * @param fileIdentifier the file path string (with optional format prefix separated by '/')
     * @return FileMetadata with parsed dataFormat and filename
     */
    public FileMetadata toFileMetadata(String fileIdentifier) {
        // Handle "file:::format" convention (serialized FileMetadata from UploaderService)
        if (fileIdentifier.contains(FileMetadata.DELIMITER)) {
            return new FileMetadata(fileIdentifier);
        }
        // Handle "format/file" convention
        int slash = fileIdentifier.indexOf('/');
        if (slash >= 0) {
            String format = fileIdentifier.substring(0, slash);
            String file = fileIdentifier.substring(slash + 1);
            return new FileMetadata(format, file);
        }
        // No prefix → Lucene/index file
        return new FileMetadata(DEFAULT_FORMAT, fileIdentifier);
    }

    /**
     * Converts a {@link FileMetadata} object back to a file identifier string.
     *
     * <ul>
     *   <li>{@code FileMetadata("parquet", "_0_1.parquet")} → {@code "parquet/_0_1.parquet"}</li>
     *   <li>{@code FileMetadata("index", "_0.cfs")} → {@code "_0.cfs"}</li>
     * </ul>
     *
     * @param fm the FileMetadata to convert
     * @return file identifier string suitable for Directory operations
     */
    public String toFileIdentifier(FileMetadata fm) {
        if (INDEX_DIRECTORY_FORMATS.contains(fm.dataFormat())) {
            return fm.file();
        }
        return fm.dataFormat() + "/" + fm.file();
    }

    // ═══════════════════════════════════════════════════════════════
    // Format-Aware Checksum Calculation
    // ═══════════════════════════════════════════════════════════════

    /**
     * Calculates the checksum for a file using the appropriate strategy based on its format.
     *
     * <p>The format is determined by parsing the file identifier string:</p>
     * <ul>
     *   <li>Lucene/index files (no prefix or "index/" prefix) → {@code CodecUtil.retrieveChecksum()}</li>
     *   <li>Non-Lucene files (e.g., "parquet/...") → full-file CRC32</li>
     * </ul>
     *
     * @param name the file identifier string (e.g., "_0.cfs" or "parquet/_0_1.parquet")
     * @return the checksum value
     * @throws IOException if checksum calculation fails
     */
    public long calculateChecksum(String name) throws IOException {
        FileMetadata fm = toFileMetadata(name);
        return calculateChecksum(fm);
    }

    /**
     * Calculates the checksum using a {@link FileMetadata} object for format-aware routing.
     *
     * <p>Delegates to the appropriate {@link org.opensearch.index.store.checksum.ChecksumHandler}
     * registered in the {@link ChecksumHandlerRegistry} for the file's data format.</p>
     *
     * @param fm the FileMetadata containing format and filename information
     * @return the checksum value
     * @throws IOException if checksum calculation fails
     */
    public long calculateChecksum(FileMetadata fm) throws IOException {
        String fileIdentifier = toFileIdentifier(fm);
        try (IndexInput input = openInput(fileIdentifier, IOContext.READONCE)) {
            return checksumRegistry.calculateChecksum(fm.dataFormat(), input);
        }
    }

    /**
     * Calculates a checksum suitable for upload verification.
     *
     * @param name the file identifier string
     * @return checksum as a string representation
     * @throws IOException if calculation fails
     */
    public String calculateUploadChecksum(String name) throws IOException {
        return Long.toString(calculateChecksum(name));
    }

    /**
     * Calculates a checksum suitable for upload verification from FileMetadata.
     *
     * @param fm the FileMetadata
     * @return checksum as a string representation
     * @throws IOException if calculation fails
     */
    public String calculateUploadChecksum(FileMetadata fm) throws IOException {
        return Long.toString(calculateChecksum(fm));
    }

    // ═══════════════════════════════════════════════════════════════
    // FileMetadata-Based Convenience Operations
    // These delegate to the inherited String-based methods from
    // SubdirectoryAwareDirectory, converting FileMetadata → String
    // ═══════════════════════════════════════════════════════════════

    /**
     * Returns the byte length of a file using FileMetadata for format routing.
     *
     * @param fm the FileMetadata containing format and filename
     * @return the length of the file in bytes
     * @throws IOException if the file cannot be accessed
     */
    public long fileLength(FileMetadata fm) throws IOException {
        return fileLength(toFileIdentifier(fm));
    }

    /**
     * Deletes a file using FileMetadata.
     *
     * @param fm the FileMetadata containing format and filename
     * @throws IOException if the file exists but could not be deleted
     */
    public void deleteFile(FileMetadata fm) throws IOException {
        deleteFile(toFileIdentifier(fm));
    }

    /**
     * Opens an IndexInput for reading using FileMetadata.
     *
     * @param fm      the FileMetadata containing format and filename
     * @param context IOContext providing performance hints for the operation
     * @return IndexInput for reading from the file
     * @throws IOException if the IndexInput cannot be created or file does not exist
     */
    public IndexInput openInput(FileMetadata fm, IOContext context) throws IOException {
        return openInput(toFileIdentifier(fm), context);
    }

    /**
     * Creates an IndexOutput for writing using FileMetadata.
     *
     * @param fm      the FileMetadata containing format and filename
     * @param context IOContext providing performance hints for the operation
     * @return IndexOutput for writing to the file
     * @throws IOException if the IndexOutput cannot be created
     */
    public IndexOutput createOutput(FileMetadata fm, IOContext context) throws IOException {
        return createOutput(toFileIdentifier(fm), context);
    }

    /**
     * Copies a file from a source directory into this composite directory using FileMetadata.
     *
     * @param fm      the FileMetadata identifying the file to copy
     * @param source  the source Directory to copy from
     * @param context IOContext providing performance hints
     * @throws IOException if the copy operation fails
     */
    public void copyFrom(FileMetadata fm, Directory source, IOContext context) throws IOException {
        String fileIdentifier = toFileIdentifier(fm);
        try (IndexInput input = source.openInput(fm.serialize(), context);
             IndexOutput output = createOutput(fileIdentifier, context)) {
            output.copyBytes(input, input.length());
        }
    }

    /**
     * Renames a file using FileMetadata.
     * Source and destination must have the same dataFormat to prevent cross-format renames.
     *
     * @param source the source FileMetadata
     * @param dest   the destination FileMetadata (must have same dataFormat)
     * @throws IOException              if rename fails
     * @throws IllegalArgumentException if source and dest have different formats
     */
    public void rename(FileMetadata source, FileMetadata dest) throws IOException {
        if (!source.dataFormat().equals(dest.dataFormat())) {
            throw new IllegalArgumentException(
                "Cannot rename across formats: " + source.dataFormat() + " -> " + dest.dataFormat()
            );
        }
        rename(toFileIdentifier(source), toFileIdentifier(dest));
    }

    /**
     * Lists all files as FileMetadata objects with format information.
     *
     * <p>This method calls {@link #listAll()} (inherited from SubdirectoryAwareDirectory)
     * and parses each result into a FileMetadata with the appropriate format.</p>
     *
     * @return array of FileMetadata for all files across all format directories
     * @throws IOException if listing fails
     */
    public FileMetadata[] listFileMetadata() throws IOException {
        String[] allFiles = listAll();
        FileMetadata[] result = new FileMetadata[allFiles.length];
        for (int i = 0; i < allFiles.length; i++) {
            result[i] = toFileMetadata(allFiles[i]);
        }
        return result;
    }

    /**
     * Gets the checksum of a local file for comparison during recovery/replication.
     * Convenience method that delegates to {@link #calculateChecksum(FileMetadata)}.
     *
     * @param fm the FileMetadata of the file
     * @return the checksum value
     * @throws IOException if checksum calculation fails
     */
    public long getChecksumOfLocalFile(FileMetadata fm) throws IOException {
        return calculateChecksum(fm);
    }

    // ═══════════════════════════════════════════════════════════════
    // Format Query Methods
    // ═══════════════════════════════════════════════════════════════

    /**
     * Returns the set of registered non-Lucene format names.
     *
     * @return unmodifiable set of format names (e.g., {"parquet", "arrow"})
     */
    public Set<String> getRegisteredFormats() {
        return registeredFormats;
    }

    /**
     * Extracts the data format name from a file identifier string.
     *
     * @param fileIdentifier the file path string
     * @return the format name (e.g., "index", "parquet")
     */
    public String getDataFormat(String fileIdentifier) {
        return toFileMetadata(fileIdentifier).dataFormat();
    }

    // ═══════════════════════════════════════════════════════════════
    // Accessor for ChecksumHandlerRegistry
    // ═══════════════════════════════════════════════════════════════

    /**
     * Returns the checksum handler registry used by this directory.
     *
     * @return the ChecksumHandlerRegistry
     */
    public ChecksumHandlerRegistry getChecksumRegistry() {
        return checksumRegistry;
    }
}
