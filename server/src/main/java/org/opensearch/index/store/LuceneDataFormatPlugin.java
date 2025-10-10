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
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.DataFormatPlugin;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.IndexingExecutionEngine;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.remote.FormatRemoteDirectory;
import org.opensearch.index.store.remote.LuceneRemoteDirectory;

import java.io.IOException;
import java.util.Set;
//
///**
// * DataFormatPlugin implementation for Lucene format support.
// * Provides Lucene-specific store directory creation and file extension handling.
// */
//public class LuceneDataFormatPlugin implements DataFormatPlugin<DataFormat.LuceneDataFormat> {
//
//    /**
//     * Singleton instance for the Lucene data format plugin
//     */
//    public static final LuceneDataFormatPlugin INSTANCE = new LuceneDataFormatPlugin();
//
//    /**
//     * Set of file extensions that Lucene format handles
//     */
//    private static final Set<String> LUCENE_EXTENSIONS = Set.of(
//        ".cfs", ".cfe", ".si", ".fnm", ".fdx", ".fdt",
//        ".tim", ".tip", ".doc", ".pos", ".pay",
//        ".nvd", ".nvm", ".dvm", ".dvd", ".tvx", ".tvd", ".tvf",
//        ".del", ".liv"
//    );
//
//    /**
//     * Private constructor to enforce singleton pattern
//     */
//    private LuceneDataFormatPlugin() {
//    }
//
//    @Override
//    public IndexingExecutionEngine<DataFormat.LuceneDataFormat> indexingEngine() {
//        // For now, return null as this is not implemented yet
//        // This will be implemented when the indexing engine is needed
//        return null;
//    }
//
//    @Override
//    public DataFormat getDataFormat() {
//        return DataFormat.LUCENE;
//    }
//
//    @Override
//    public FormatStoreDirectory createFormatStoreDirectory(
//        IndexSettings indexSettings,
//        ShardPath shardPath
//    ) throws IOException {
//        // Create a NIOFSDirectory for the lucene subdirectory
//        NIOFSDirectory luceneDirectory = new NIOFSDirectory(
//            shardPath.getDataPath().resolve("lucene")
//        );
//
//        // Create and return LuceneStoreDirectory wrapping the NIOFSDirectory
//        return new LuceneStoreDirectory(
//            DataFormat.LUCENE,
//            shardPath.getDataPath(),
//            luceneDirectory
//        );
//    }
//
//    @Override
//    public FormatRemoteDirectory createFormatRemoteDirectory(
//        IndexSettings indexSettings,
//        BlobContainer baseBlobContainer,
//        String remoteBasePath
//    ) throws IOException {
//        // For Lucene format, we need to use the second method with RemoteDirectory
//        // This method should not be called directly for Lucene format
//        throw new UnsupportedOperationException(
//            "Lucene format requires RemoteDirectory wrapper. Use createFormatRemoteDirectory(IndexSettings, RemoteDirectory, String) instead."
//        );
//    }
//
//    @Override
//    public FormatRemoteDirectory createFormatRemoteDirectory(
//        IndexSettings indexSettings,
//        RemoteDirectory remoteDirectory,
//        String remoteBasePath
//    ) throws IOException {
//        // Create LuceneRemoteDirectory wrapping the existing RemoteDirectory
//        // This maintains backward compatibility with existing Lucene remote operations
//        Logger logger = LogManager.getLogger(LuceneRemoteDirectory.class);
//        return new LuceneRemoteDirectory(remoteBasePath, remoteDirectory, logger);
//    }
//
//    @Override
//    public boolean supportsRemoteStorage() {
//        return true;
//    }
//
//    @Override
//    public String getRemotePathSuffix() {
//        return "lucene";
//    }
//
//    @Override
//    public Set<String> getSupportedExtensions() {
//        return LUCENE_EXTENSIONS;
//    }
//}
