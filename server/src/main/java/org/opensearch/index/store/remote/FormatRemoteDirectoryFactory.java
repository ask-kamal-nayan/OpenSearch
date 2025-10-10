/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote;

import org.apache.logging.log4j.Logger;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.DataFormatPlugin;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.store.RemoteDirectory;
import org.opensearch.plugins.PluginsService;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Factory for creating format-specific remote directories using plugin discovery.
 * This factory discovers available DataFormatPlugin instances and creates
 * FormatRemoteDirectory instances for each supported format.
 */
public class FormatRemoteDirectoryFactory {

    private final Logger logger;

    /**
     * Creates a new FormatRemoteDirectoryFactory.
     * 
     * @param logger Logger for error reporting and debugging
     */
    public FormatRemoteDirectoryFactory(Logger logger) {
        this.logger = logger;
    }

    /**
     * Creates a map of DataFormat to FormatRemoteDirectory using plugin discovery.
     * Discovers all available DataFormatPlugin instances and creates format remote directories
     * for each plugin. Handles plugin failures gracefully and ensures Lucene format
     * is always available for backward compatibility.
     * 
     * @param indexSettings           the index settings
     * @param baseBlobContainer      the base blob container for remote storage
     * @param remoteDirectory        the existing remote directory for Lucene compatibility
     * @param pluginsService         the plugins service for plugin discovery
     * @return Map of DataFormat to FormatRemoteDirectory with all discovered formats
     * @throws IOException if directory creation fails
     */
    public Map<DataFormat, FormatRemoteDirectory> createFormatRemoteDirectories(
            IndexSettings indexSettings,
            BlobContainer baseBlobContainer,
            RemoteDirectory remoteDirectory,
            PluginsService pluginsService) throws IOException {

        Map<DataFormat, FormatRemoteDirectory> formatDirectories = new HashMap<>();
        List<DataFormatPlugin> plugins = pluginsService.filterPlugins(DataFormatPlugin.class);

        logger.debug("Discovered {} data format plugins for remote directories", plugins.size());

        // Iterate through plugins and create format remote directories
        for (DataFormatPlugin plugin : plugins) {
            try {
                DataFormat format = plugin.getDataFormat();

                // Handle duplicate format plugins by using first plugin and logging warning
                if (formatDirectories.containsKey(format)) {
                    logger.warn("Multiple plugins found for format {}, using first plugin for remote directory", format.name());
                    continue;
                }

                // Call plugin's createFormatRemoteDirectory method (to be added to DataFormatPlugin interface)
                FormatRemoteDirectory directory;
                
                // For now, use format-specific logic until DataFormatPlugin interface is extended
                if (format == DataFormat.LUCENE) {
                    // Create LuceneRemoteDirectory wrapping the existing RemoteDirectory
                    directory = new LuceneRemoteDirectory(baseBlobContainer.path().buildAsString(), remoteDirectory, logger);
                } else {
                    // Create GenericRemoteDirectory for non-Lucene formats (skip for now until we fix its constructor)
                    logger.warn("Skipping non-Lucene format {} until GenericRemoteDirectory is fixed", format.name());
                    continue;
                }

                formatDirectories.put(format, directory);

                logger.debug("Created remote directory for format: {} at path: {}",
                        format.name(), directory.getRemoteBasePath());

            } catch (Exception e) {
                // Wrap plugin creation in try-catch blocks to handle individual plugin failures
                // Add comprehensive error context including plugin class and format information
                String formatName = "unknown";
                try {
                    formatName = plugin.getDataFormat().name();
                } catch (Exception formatException) {
                    // If we can't even get the format name, log that too
                    logger.error("Failed to get format name from plugin: {}", plugin.getClass().getSimpleName(),
                            formatException);
                }

                logger.error("Failed to create remote directory for plugin: {} (format: {}). Error: {}",
                        plugin.getClass().getSimpleName(), formatName, e.getMessage(), e);

                // Continue with other plugins if one fails, logging error details
                // This ensures that one failing plugin doesn't break the entire system
            }
        }

        // Ensure Lucene format is always available by creating default
        // LuceneRemoteDirectory if needed
        if (!formatDirectories.containsKey(DataFormat.LUCENE)) {
            logger.warn("No Lucene format plugin found, creating default LuceneRemoteDirectory for remote storage");
            try {
                FormatRemoteDirectory defaultLuceneRemoteDirectory = createDefaultLuceneRemoteDirectory(
                        baseBlobContainer, 
                        remoteDirectory);
                formatDirectories.put(DataFormat.LUCENE, defaultLuceneRemoteDirectory);
                logger.debug("Created default Lucene remote directory at path: {}", 
                        defaultLuceneRemoteDirectory.getRemoteBasePath());
            } catch (IOException e) {
                logger.error("Failed to create default Lucene remote directory for backward compatibility", e);
                throw new MultiFormatRemoteException(
                        "Cannot create default Lucene remote directory for backward compatibility",
                        DataFormat.LUCENE,
                        "createDefaultRemoteDirectory",
                        baseBlobContainer.path().buildAsString(),
                        e);
            }
        }

        return formatDirectories;
    }

    /**
     * Creates a default LuceneRemoteDirectory for fallback Lucene support.
     * This method ensures backward compatibility when no Lucene plugin is found.
     * 
     * @param baseBlobContainer the base blob container for remote storage
     * @param remoteDirectory   the existing remote directory to wrap
     * @return FormatRemoteDirectory instance for Lucene format
     * @throws IOException if directory creation fails
     */
    private FormatRemoteDirectory createDefaultLuceneRemoteDirectory(
            BlobContainer baseBlobContainer, 
            RemoteDirectory remoteDirectory) throws IOException {
        
        // Create LuceneRemoteDirectory wrapping existing RemoteDirectory
        // This ensures backward compatibility when no Lucene plugin is found
        return new LuceneRemoteDirectory(baseBlobContainer.path().buildAsString(), remoteDirectory, logger);
    }

    /**
     * Creates format remote directories with rate limiting suppliers.
     * This is an alternative method that provides rate limiting functionality
     * similar to what's used in RemoteSegmentStoreDirectoryFactory.
     * 
     * @param indexSettings                       the index settings
     * @param baseBlobContainer                  the base blob container for remote storage
     * @param uploadRateLimiterSupplier         supplier for upload rate limiting
     * @param lowPriorityUploadRateLimiterSupplier supplier for low priority upload rate limiting
     * @param downloadRateLimiterSupplier       supplier for download rate limiting
     * @param pluginsService                    the plugins service for plugin discovery
     * @return Map of DataFormat to FormatRemoteDirectory with all discovered formats
     * @throws IOException if directory creation fails
     */
    public Map<DataFormat, FormatRemoteDirectory> createFormatRemoteDirectoriesWithRateLimiting(
            IndexSettings indexSettings,
            BlobContainer baseBlobContainer,
            Supplier<Long> uploadRateLimiterSupplier,
            Supplier<Long> lowPriorityUploadRateLimiterSupplier,
            Supplier<Long> downloadRateLimiterSupplier,
            PluginsService pluginsService) throws IOException {

        // Create a simple RemoteDirectory for Lucene compatibility without rate limiting for now
        // TODO: Fix rate limiting integration once RemoteDirectory constructor signature is clarified
        RemoteDirectory remoteDirectory = new RemoteDirectory(baseBlobContainer);

        return createFormatRemoteDirectories(indexSettings, baseBlobContainer, remoteDirectory, pluginsService);
    }
}
