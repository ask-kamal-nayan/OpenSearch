/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

@ExperimentalApi
public class IndexFileDeleter {

    private static final Logger logger = LogManager.getLogger(IndexFileDeleter.class);

    private final Map<String,Map<String, AtomicInteger>> fileRefCounts = new ConcurrentHashMap<>();
    private final CompositeEngine compositeEngine;

    public IndexFileDeleter(CompositeEngine compositeEngine, CatalogSnapshot initialCatalogSnapshot, ShardPath shardPath) throws IOException {
        this(compositeEngine, initialCatalogSnapshot, shardPath, true);
    }

    public IndexFileDeleter(CompositeEngine compositeEngine, CatalogSnapshot initialCatalogSnapshot, ShardPath shardPath, boolean deleteUnreferenced) throws IOException {
        this.compositeEngine = compositeEngine;
        if (initialCatalogSnapshot != null) {
            addFileReferences(initialCatalogSnapshot);
            if (deleteUnreferenced) {
                deleteUnreferencedFiles(shardPath);
            } else {
                logger.info("[INDEX_FILE_DELETER] Skipping deleteUnreferencedFiles (deleteUnreferenced=false), shardPath={}",
                    shardPath.getDataPath());
            }
        }
    }

    public synchronized void addFileReferences(CatalogSnapshot snapshot) {
        Map<String, Collection<String>> dfSegregatedFiles = segregateFilesByFormat(snapshot);
        for (Map.Entry<String, Collection<String>> entry : dfSegregatedFiles.entrySet()) {
            String dataFormat = entry.getKey();
            Map<String, AtomicInteger> dfFileRefCounts = fileRefCounts.computeIfAbsent(dataFormat, k -> new HashMap<>());
            Collection<String> files = entry.getValue();
            for (String file : files) {
                dfFileRefCounts.computeIfAbsent(file, k -> new AtomicInteger(0)).incrementAndGet();
            }
        }
    }

    public synchronized void removeFileReferences(CatalogSnapshot snapshot) {
        Map<String, Collection<String>> dfSegregatedFiles = segregateFilesByFormat(snapshot);
        Map<String, Collection<String>> dfFilesToDelete = new HashMap<>();

        for (Map.Entry<String, Collection<String>> entry : dfSegregatedFiles.entrySet()) {
            String dataFormat = entry.getKey();
            Collection<String> filesToDelete = new HashSet<>();
            Map<String, AtomicInteger> dfFileRefCounts = fileRefCounts.get(dataFormat);
            if (dfFileRefCounts != null) {
                Collection<String> files = entry.getValue();
                for (String file : files) {
                    AtomicInteger refCount = dfFileRefCounts.get(file);
                    if (refCount != null && refCount.decrementAndGet() == 0) {
                        dfFileRefCounts.remove(file);
                        filesToDelete.add(file);
                    }
                }
            }
            dfFilesToDelete.put(dataFormat, filesToDelete);
        }

        if (!dfFilesToDelete.isEmpty()) {
            logger.info("[INDEX_FILE_DELETER] removeFileReferences: filesToDelete={}", dfFilesToDelete);
            deleteUnreferencedFiles(dfFilesToDelete);
        }
    }

    private Map<String, Collection<String>> segregateFilesByFormat(CatalogSnapshot snapshot) {
        Map<String, Collection<String>> dfSegregatedFiles = new HashMap<>();
        Set<String> dataFormats = snapshot.getDataFormats();
        for (String dataFormat : dataFormats) {
            Collection<String> dfFiles = new HashSet<>();
            Collection<WriterFileSet> fileSets = snapshot.getSearchableFiles(dataFormat);
            for (WriterFileSet fileSet : fileSets) {
                Path directory = Path.of(fileSet.getDirectory());
                for (String file : fileSet.getFiles()) {
                    // ToDo: @Shreyansh update this to relative path
                    dfFiles.add(directory.resolve(file).toAbsolutePath().normalize().toString());
                }
            }
            dfSegregatedFiles.put(dataFormat, dfFiles);
        }
        return dfSegregatedFiles;
    }

    private void deleteUnreferencedFiles(ShardPath shardPath) throws IOException {
        if (fileRefCounts.isEmpty())
            return;
        Map<String, Collection<String>> dfFilesToDelete = new HashMap<>();
        for (Map.Entry<String, Map<String, AtomicInteger>> entry : fileRefCounts.entrySet()) {
            String dataFormat = entry.getKey();
            Collection<String> referencedFiles = entry.getValue().keySet();
            Collection<String> filesToDelete = new HashSet<>();
            Path dataFormatPath = shardPath.getDataPath().resolve(dataFormat);
            if (!Files.exists(dataFormatPath)) continue;

            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataFormatPath, "*." + dataFormat)) {
                StreamSupport.stream(stream.spliterator(), false)
                        .map(p -> p.toAbsolutePath().normalize().toString())
                        .filter((file) -> (!referencedFiles.contains(file)))
                        .forEach(filesToDelete::add);
            }
            if (!filesToDelete.isEmpty()) {
                dfFilesToDelete.put(dataFormat, filesToDelete);
            }
            logger.info("[INDEX_FILE_DELETER] deleteUnreferencedFiles: format={}, referenced={}, toDelete={}, path={}",
                dataFormat, referencedFiles.size(), filesToDelete.size(), dataFormatPath);
        }
        deleteUnreferencedFiles(dfFilesToDelete);
    }

    private void deleteUnreferencedFiles(Map<String, Collection<String>> dfFilesToDelete) {
        try {
            if (dfFilesToDelete.isEmpty())
                return;
            logger.info("[INDEX_FILE_DELETER] Deleting unreferenced files: {}", dfFilesToDelete);
            compositeEngine.notifyDelete(dfFilesToDelete);
        } catch (Exception e) {
            logger.warn("[INDEX_FILE_DELETER] Failed to delete unreferenced files: {}, error: {}", dfFilesToDelete, e.getMessage());
        }
    }

    @Override
    public String toString() {
        return "IndexFileDeleter{fileRefCounts=" + fileRefCounts + "}";
    }

    // Used only for testing
    public Map<String, Map<String, AtomicInteger>> getFileRefCounts() {
        return Map.copyOf(fileRefCounts);
    }
}
