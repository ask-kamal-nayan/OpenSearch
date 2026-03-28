/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.checksum;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.annotation.PublicApi;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry that maps data format names to their {@link ChecksumHandler} implementations.
 *
 * <p>This registry follows the same pattern as the {@code CompositeStoreDirectory}'s
 * registered formats discovery. It is built during directory creation by the
 * {@code DefaultCompositeStoreDirectoryFactory}, which collects handlers from
 * {@code DataSourcePlugin} implementations.</p>
 *
 * <p><strong>Built-in handlers:</strong></p>
 * <ul>
 *   <li>{@code "lucene"} → {@link LuceneChecksumHandler} (always registered)</li>
 * </ul>
 *
 * <p><strong>Fallback behavior:</strong></p>
 * <p>If a format has no registered handler, the {@link GenericCRC32ChecksumHandler}
 * is used as a default fallback.</p>
 *
 * @opensearch.api
 */
@PublicApi(since = "3.0.0")
public class ChecksumHandlerRegistry {

    private static final Logger logger = LogManager.getLogger(ChecksumHandlerRegistry.class);

    /**
     * Map from format name to its checksum handler.
     */
    private final Map<String, ChecksumHandler> handlers = new ConcurrentHashMap<>();

    /**
     * Default handler used when no format-specific handler is registered.
     */
    private final ChecksumHandler defaultHandler = new GenericCRC32ChecksumHandler();

    /**
     * Creates a new ChecksumHandlerRegistry with the built-in Lucene handler pre-registered.
     */
    public ChecksumHandlerRegistry() {
        registerHandler(new LuceneChecksumHandler());
        logger.debug("ChecksumHandlerRegistry initialized with built-in Lucene handler");
    }

    /**
     * Registers a checksum handler for a specific format.
     *
     * <p>If a handler is already registered for the same format name, it will be replaced.</p>
     *
     * @param handler the checksum handler to register
     * @throws IllegalArgumentException if handler is null
     */
    public void registerHandler(ChecksumHandler handler) {
        if (handler == null) {
            throw new IllegalArgumentException("ChecksumHandler cannot be null");
        }
        handlers.put(handler.getFormatName(), handler);
        logger.debug("Registered checksum handler for format: {} ({})", handler.getFormatName(), handler.getClass().getSimpleName());
    }

    /**
     * Returns the checksum handler for the given format name.
     *
     * <p>If no handler is registered for the format, returns the default
     * {@link GenericCRC32ChecksumHandler}.</p>
     *
     * @param formatName the format name (e.g., "lucene", "parquet", "arrow")
     * @return the registered handler, or the default handler if none is registered
     */
    public ChecksumHandler getHandler(String formatName) {
        return handlers.getOrDefault(formatName, defaultHandler);
    }

    /**
     * Calculates the checksum for a file using the appropriate format-specific handler.
     *
     * @param formatName the data format name
     * @param input      the IndexInput to calculate the checksum from
     * @return the checksum value
     * @throws IOException if checksum calculation fails
     */
    public long calculateChecksum(String formatName, IndexInput input) throws IOException {
        return getHandler(formatName).calculateChecksum(input);
    }

    /**
     * Calculates the upload checksum for a file using the appropriate format-specific handler.
     *
     * @param formatName the data format name
     * @param input      the IndexInput to calculate the checksum from
     * @return checksum as a string representation
     * @throws IOException if checksum calculation fails
     */
    public String calculateUploadChecksum(String formatName, IndexInput input) throws IOException {
        return getHandler(formatName).calculateUploadChecksum(input);
    }

    /**
     * Returns the set of registered format names (excluding the default handler).
     *
     * @return unmodifiable set of registered format names
     */
    public Set<String> getRegisteredFormats() {
        return Collections.unmodifiableSet(handlers.keySet());
    }

    /**
     * Checks if a specific format has a registered handler.
     *
     * @param formatName the format name to check
     * @return true if a handler is registered for this format
     */
    public boolean hasHandler(String formatName) {
        return handlers.containsKey(formatName);
    }
}
