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
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A Lucene Directory implementation that handles files organized in subdirectories
 * within the shard data path.
 *
 * <p>This directory wrapper enables file operations across subdirectories. It resolves
 * paths, creates necessary directory structures, and delegates actual file operations
 * to appropriate filesystem locations.</p>
 *
 * <p><strong>Routing rules:</strong></p>
 * <ul>
 *   <li>Plain filenames (no '/') → resolved to {@code shardPath.resolveIndex()} (the index/ directory)</li>
 *   <li>Path-style filenames (contain '/') → resolved to {@code shardPath.getDataPath().resolve(name)}</li>
 * </ul>
 *
 * <p>This class is extracted from {@code SubdirectoryAwareStore} to enable reuse
 * by both {@code SubdirectoryAwareStore} and {@code CompositeStoreDirectory}.</p>
 *
 * @opensearch.api
 */
@PublicApi(since = "3.0.0")
public class SubdirectoryAwareDirectory extends FilterDirectory {

    private static final Logger logger = LogManager.getLogger(SubdirectoryAwareDirectory.class);

    /**
     * Standard subdirectories that are excluded from file discovery.
     * These are managed by other parts of OpenSearch and should not be
     * returned by {@link #listAll()}.
     */
    private static final Set<String> EXCLUDED_SUBDIRECTORIES = Set.of("index/", "translog/", "_state/");

    protected final ShardPath shardPath;

    /**
     * Constructs a SubdirectoryAwareDirectory.
     *
     * @param delegate  the underlying Lucene Directory (typically FSDirectory on the index/ folder)
     * @param shardPath the shard path for resolving subdirectory locations
     */
    public SubdirectoryAwareDirectory(Directory delegate, ShardPath shardPath) {
        super(delegate);
        this.shardPath = shardPath;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        return super.openInput(parseFilePath(name), context);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        String targetFilePath = parseFilePath(name);
        Path targetFile = Path.of(targetFilePath);
        if (targetFile.getParent() != null) {
            Files.createDirectories(targetFile.getParent());
        }
        return super.createOutput(targetFilePath, context);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        super.deleteFile(parseFilePath(name));
    }

    @Override
    public long fileLength(String name) throws IOException {
        return super.fileLength(parseFilePath(name));
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        super.sync(names.stream().map(this::parseFilePath).collect(Collectors.toList()));
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        super.rename(parseFilePath(source), parseFilePath(dest));
    }

    @Override
    public String[] listAll() throws IOException {
        // Get files from the delegate (regular index files)
        String[] delegateFiles = super.listAll();
        Set<String> allFiles = new HashSet<>(Arrays.asList(delegateFiles));

        // Add subdirectory files by scanning all subdirectories
        addSubdirectoryFiles(allFiles);

        return allFiles.stream().sorted().toArray(String[]::new);
    }

    /**
     * Walks the shard data path to discover files in subdirectories.
     * Excludes standard directories: index/, translog/, _state/
     *
     * @param allFiles the set to add discovered file paths to
     * @throws IOException if file tree walking fails
     */
    protected void addSubdirectoryFiles(Set<String> allFiles) throws IOException {
        Path dataPath = shardPath.getDataPath();
        if (!Files.exists(dataPath)) {
            return;
        }

        Files.walkFileTree(dataPath, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                if (attrs.isRegularFile()) {
                    Path relativePath = dataPath.relativize(file);
                    // Only add files that are in subdirectories (have a parent directory)
                    if (relativePath.getParent() != null) {
                        String relativePathStr = relativePath.toString();
                        // Exclude index dir (handled by super.listAll()), translog dir, and _state dir
                        if (EXCLUDED_SUBDIRECTORIES.stream().noneMatch(relativePathStr::startsWith)) {
                            allFiles.add(relativePathStr);
                        }
                    }
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException e) throws IOException {
                if (e instanceof NoSuchFileException) {
                    logger.debug("Skipping inaccessible file during directory scan: {}", file);
                    return FileVisitResult.CONTINUE;
                }
                throw e;
            }
        });
    }

    /**
     * Resolves a file identifier to an absolute filesystem path.
     *
     * <ul>
     *   <li>{@code "subdir/file.ext"} → {@code shardPath.getDataPath().resolve("subdir/file.ext")}</li>
     *   <li>{@code "file.ext"} → {@code shardPath.resolveIndex().resolve("file.ext")}</li>
     * </ul>
     *
     * @param fileName the file identifier (may contain '/' for subdirectory files)
     * @return the absolute filesystem path as a string
     */
    protected String parseFilePath(String fileName) {
        if (Path.of(fileName).getParent() != null) {
            // File has a path prefix (e.g., "parquet/_0_1.parquet") → resolve against data path
            return shardPath.getDataPath().resolve(fileName).toString();
        } else {
            // Simple filename (e.g., "_0.cfs") → resolve relative to the shard's index directory
            return shardPath.resolveIndex().resolve(fileName).toString();
        }
    }

    /**
     * Returns the shard path associated with this directory.
     *
     * @return the ShardPath
     */
    public ShardPath getShardPath() {
        return shardPath;
    }
}
