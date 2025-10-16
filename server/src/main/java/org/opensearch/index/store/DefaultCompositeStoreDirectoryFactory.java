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
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.coord.Any;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.PluginsService;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Default implementation of CompositeStoreDirectoryFactory that provides
 * plugin-based format discovery and fallback behavior.
 *
 * This factory:
 * - Discovers DataFormat plugins through PluginsService
 * - Creates CompositeStoreDirectory with all discovered formats
 * - Provides fallback to default formats (Lucene, Text) if no plugins found
 * - Handles errors gracefully with detailed logging
 * - Maintains backward compatibility with existing directory creation
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DefaultCompositeStoreDirectoryFactory implements CompositeStoreDirectoryFactory {

    private static final Logger logger = LogManager.getLogger(DefaultCompositeStoreDirectoryFactory.class);

    /**
     * Default formats used when no plugins are discovered.
     * Includes Lucene (primary format) and Text (fallback format).
     */
    private static final List<DataFormat> DEFAULT_FORMATS = Arrays.asList(
        DataFormat.PARQUET
    );

    /**
     * Creates a new CompositeStoreDirectory with plugin-based format discovery.
     *
     * Process:
     * 1. Log directory creation start
     * 2. Attempt plugin-based format discovery
     * 3. Create CompositeStoreDirectory with discovered formats
     * 4. Fallback to default formats if plugin discovery fails
     * 5. Handle errors with detailed logging and context
     *
     * @param indexSettings the shard's index settings
     * @param shardPath the path the shard is using
     * @param pluginsService service for discovering DataFormat plugins
     * @return a new CompositeStoreDirectory instance
     * @throws IOException if directory creation fails
     */
    @Override
    public CompositeStoreDirectory newCompositeStoreDirectory(
        IndexSettings indexSettings,
        ShardPath shardPath,
        PluginsService pluginsService
    ) throws IOException {

        if (logger.isDebugEnabled()) {
            logger.debug("Creating CompositeStoreDirectory for shard: {} at path: {}",
                shardPath.getShardId(), shardPath.getDataPath());
        }

        try {
            // Attempt to create CompositeStoreDirectory with plugin discovery
            // This uses the auto-discovery constructor that finds DataFormat plugins
            CompositeStoreDirectory compositeDirectory = new CompositeStoreDirectory(
                indexSettings,
                pluginsService,
                shardPath,
                logger
            );

            if (logger.isDebugEnabled()) {
                logger.debug("Successfully created CompositeStoreDirectory for shard: {} with plugin discovery",
                    shardPath.getShardId());
            }

            return compositeDirectory;

        } catch (Exception pluginException) {
            // Log plugin discovery failure and attempt fallback
            logger.warn("Plugin-based CompositeStoreDirectory creation failed for shard: {}, attempting fallback. Error: {}",
                shardPath.getShardId(), pluginException.getMessage(), pluginException);

            try {
                // Fallback to manual format specification with default formats
                Any defaultDataFormats = new Any(DEFAULT_FORMATS);
                CompositeStoreDirectory fallbackDirectory = new CompositeStoreDirectory(
                    indexSettings,
                    pluginsService,
                    defaultDataFormats,
                    shardPath,
                    logger
                );

                logger.info("Created fallback CompositeStoreDirectory for shard: {} with default formats: {}",
                    shardPath.getShardId(), DEFAULT_FORMATS);

                return fallbackDirectory;

            } catch (Exception fallbackException) {
                // Both plugin discovery and fallback failed
                logger.error("Failed to create CompositeStoreDirectory for shard: {} - both plugin discovery and fallback failed",
                    shardPath.getShardId(), fallbackException);

                // Add original plugin exception as suppressed for full context
                fallbackException.addSuppressed(pluginException);

                throw new IOException(
                    String.format("Failed to create CompositeStoreDirectory for shard %s: plugin discovery failed (%s), fallback failed (%s)",
                        shardPath.getShardId(),
                        pluginException.getMessage(),
                        fallbackException.getMessage()
                    ),
                    fallbackException
                );
            }
        }
    }

    /**
     * Creates a CompositeStoreDirectory with specific formats (for testing/special cases).
     * This method bypasses plugin discovery and uses the provided formats directly.
     *
     * @param indexSettings the shard's index settings
     * @param shardPath the path the shard is using
     * @param pluginsService service for creating format directories
     * @param dataFormats specific formats to support
     * @return a new CompositeStoreDirectory instance with specified formats
     * @throws IOException if directory creation fails
     */
    public CompositeStoreDirectory newCompositeStoreDirectoryWithFormats(
        IndexSettings indexSettings,
        ShardPath shardPath,
        PluginsService pluginsService,
        Any dataFormats
    ) throws IOException {

        if (logger.isDebugEnabled()) {
            logger.debug("Creating CompositeStoreDirectory for shard: {} with specific formats: {}",
                shardPath.getShardId(), dataFormats.getDataFormats());
        }

        try {
            CompositeStoreDirectory compositeDirectory = new CompositeStoreDirectory(
                indexSettings,
                pluginsService,
                dataFormats,
                shardPath,
                logger
            );

            if (logger.isDebugEnabled()) {
                logger.debug("Successfully created CompositeStoreDirectory for shard: {} with specific formats",
                    shardPath.getShardId());
            }

            return compositeDirectory;

        } catch (Exception e) {
            logger.error("Failed to create CompositeStoreDirectory with specific formats for shard: {}",
                shardPath.getShardId(), e);
            throw new IOException("Failed to create CompositeStoreDirectory with specific formats", e);
        }
    }
}
