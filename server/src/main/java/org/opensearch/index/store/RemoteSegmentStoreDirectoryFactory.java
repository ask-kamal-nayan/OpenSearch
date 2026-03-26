/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.store.Directory;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.remote.RemoteStorePathStrategy;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.lockmanager.RemoteStoreLockManager;
import org.opensearch.index.store.lockmanager.RemoteStoreLockManagerFactory;
import org.opensearch.index.store.remote.CompositeRemoteDirectory;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.plugins.PluginsService;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static org.opensearch.index.remote.RemoteStoreEnums.DataCategory.SEGMENTS;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.DATA;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.METADATA;

/**
 * Factory for a remote store directory.
 *
 * <p>This factory creates a single {@link RemoteSegmentStoreDirectory} for both optimized and non-optimized indices.
 * The decision of which underlying data directory to use is made here:
 * <ul>
 *   <li>Optimized index → {@link CompositeRemoteDirectory} (format-aware, multi-blobstore routing)</li>
 *   <li>Non-optimized index → {@link RemoteDirectory} (single blobstore)</li>
 * </ul>
 *
 * @opensearch.api
 */
@PublicApi(since = "2.3.0")
public class RemoteSegmentStoreDirectoryFactory implements IndexStorePlugin.DirectoryFactory {
    private final Supplier<RepositoriesService> repositoriesService;
    private final String segmentsPathFixedPrefix;
    private final ThreadPool threadPool;
    private final PluginsService pluginsService;

    public RemoteSegmentStoreDirectoryFactory(
        Supplier<RepositoriesService> repositoriesService,
        ThreadPool threadPool,
        String segmentsPathFixedPrefix
    ) {
        this(repositoriesService, threadPool, segmentsPathFixedPrefix, null);
    }

    public RemoteSegmentStoreDirectoryFactory(
        Supplier<RepositoriesService> repositoriesService,
        ThreadPool threadPool,
        String segmentsPathFixedPrefix,
        PluginsService pluginsService
    ) {
        this.repositoriesService = repositoriesService;
        this.segmentsPathFixedPrefix = segmentsPathFixedPrefix;
        this.threadPool = threadPool;
        this.pluginsService = pluginsService;
    }

    @Override
    public Directory newDirectory(IndexSettings indexSettings, ShardPath path) throws IOException {
        String repositoryName = indexSettings.getRemoteStoreRepository();
        String indexUUID = indexSettings.getIndex().getUUID();
        return newDirectory(
            repositoryName,
            indexUUID,
            path.getShardId(),
            indexSettings.getRemoteStorePathStrategy(),
            null,
            RemoteStoreUtils.isServerSideEncryptionEnabledIndex(indexSettings.getIndexMetadata()),
            indexSettings.isOptimizedIndex()
        );
    }

    public Directory newDirectory(String repositoryName, String indexUUID, ShardId shardId, RemoteStorePathStrategy pathStrategy)
        throws IOException {
        return newDirectory(repositoryName, indexUUID, shardId, pathStrategy, null, false, false);
    }

    public Directory newDirectory(
        String repositoryName,
        String indexUUID,
        ShardId shardId,
        RemoteStorePathStrategy pathStrategy,
        String indexFixedPrefix
    ) throws IOException {
        return newDirectory(repositoryName, indexUUID, shardId, pathStrategy, indexFixedPrefix, false, false);
    }

    public Directory newDirectory(
        String repositoryName,
        String indexUUID,
        ShardId shardId,
        RemoteStorePathStrategy pathStrategy,
        String indexFixedPrefix,
        boolean isServerSideEncryptionEnabled
    ) throws IOException {
        return newDirectory(repositoryName, indexUUID, shardId, pathStrategy, indexFixedPrefix, isServerSideEncryptionEnabled, false);
    }

    /**
     * Creates a unified RemoteSegmentStoreDirectory for both optimized and non-optimized indices.
     *
     * <p>The only difference between the two modes is the data directory:
     * <ul>
     *   <li>Optimized: {@link CompositeRemoteDirectory} — routes files to format-specific BlobContainers</li>
     *   <li>Non-optimized: {@link RemoteDirectory} — single BlobContainer at baseBlobPath</li>
     * </ul>
     *
     * @param repositoryName               the name of the remote store repository
     * @param indexUUID                     the UUID of the index
     * @param shardId                       the shard ID
     * @param pathStrategy                  the path strategy for remote store
     * @param indexFixedPrefix              optional fixed prefix for the index path
     * @param isServerSideEncryptionEnabled whether server-side encryption is enabled
     * @param isOptimizedIndex              whether this is an optimized (composite) index
     * @return the created RemoteSegmentStoreDirectory
     * @throws IOException if directory creation fails
     */
    public Directory newDirectory(
        String repositoryName,
        String indexUUID,
        ShardId shardId,
        RemoteStorePathStrategy pathStrategy,
        String indexFixedPrefix,
        boolean isServerSideEncryptionEnabled,
        boolean isOptimizedIndex
    ) throws IOException {
        assert Objects.nonNull(pathStrategy);
        Repository repository = repositoriesService.get().repository(repositoryName);
        try {
            assert repository instanceof BlobStoreRepository : "repository should be instance of BlobStoreRepository";
            BlobStoreRepository blobStoreRepository = ((BlobStoreRepository) repository);
            BlobPath repositoryBasePath = blobStoreRepository.basePath();
            String shardIdStr = String.valueOf(shardId.id());
            Map<String, String> pendingDownloadMergedSegments = new ConcurrentHashMap<>();

            // Derive the path for data directory of SEGMENTS
            RemoteStorePathStrategy.ShardDataPathInput dataPathInput = RemoteStorePathStrategy.ShardDataPathInput.builder()
                .basePath(repositoryBasePath)
                .indexUUID(indexUUID)
                .shardId(shardIdStr)
                .dataCategory(SEGMENTS)
                .dataType(DATA)
                .fixedPrefix(segmentsPathFixedPrefix)
                .indexFixedPrefix(indexFixedPrefix)
                .build();
            BlobPath dataPath = pathStrategy.generatePath(dataPathInput);

            // Create the appropriate data directory based on whether this is an optimized index
            RemoteDirectory dataDirectory = isOptimizedIndex
                ? new CompositeRemoteDirectory(
                    blobStoreRepository.blobStore(isServerSideEncryptionEnabled),
                    dataPath,
                    blobStoreRepository::maybeRateLimitRemoteUploadTransfers,
                    blobStoreRepository::maybeRateLimitLowPriorityRemoteUploadTransfers,
                    blobStoreRepository::maybeRateLimitRemoteDownloadTransfers,
                    blobStoreRepository::maybeRateLimitLowPriorityDownloadTransfers,
                    pendingDownloadMergedSegments,
                    LogManager.getLogger("index.store.remote.composite." + shardId),
                    pluginsService
                )
                : new RemoteDirectory(
                    blobStoreRepository.blobStore(isServerSideEncryptionEnabled).blobContainer(dataPath),
                    blobStoreRepository::maybeRateLimitRemoteUploadTransfers,
                    blobStoreRepository::maybeRateLimitLowPriorityRemoteUploadTransfers,
                    blobStoreRepository::maybeRateLimitRemoteDownloadTransfers,
                    blobStoreRepository::maybeRateLimitLowPriorityDownloadTransfers,
                    pendingDownloadMergedSegments
                );

            // Derive the path for metadata directory of SEGMENTS
            RemoteStorePathStrategy.ShardDataPathInput mdPathInput = RemoteStorePathStrategy.ShardDataPathInput.builder()
                .basePath(repositoryBasePath)
                .indexUUID(indexUUID)
                .shardId(shardIdStr)
                .dataCategory(SEGMENTS)
                .dataType(METADATA)
                .fixedPrefix(segmentsPathFixedPrefix)
                .indexFixedPrefix(indexFixedPrefix)
                .build();
            BlobPath mdPath = pathStrategy.generatePath(mdPathInput);
            RemoteDirectory metadataDirectory = new RemoteDirectory(
                blobStoreRepository.blobStore(isServerSideEncryptionEnabled).blobContainer(mdPath)
            );

            // The path for lock is derived within the RemoteStoreLockManagerFactory
            RemoteStoreLockManager mdLockManager = RemoteStoreLockManagerFactory.newLockManager(
                repositoriesService.get(),
                repositoryName,
                indexUUID,
                shardIdStr,
                pathStrategy,
                segmentsPathFixedPrefix,
                indexFixedPrefix
            );

            return new RemoteSegmentStoreDirectory(
                dataDirectory,
                metadataDirectory,
                mdLockManager,
                threadPool,
                shardId,
                pendingDownloadMergedSegments
            );
        } catch (RepositoryMissingException e) {
            throw new IllegalArgumentException("Repository should be created before creating index with remote_store enabled setting", e);
        }
    }

    public Supplier<RepositoriesService> getRepositoriesService() {
        return this.repositoriesService;
    }
}
