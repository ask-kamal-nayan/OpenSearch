/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Extensible checksum calculation framework for multi-format data files.
 *
 * <p>This package provides a Strategy pattern-based approach to checksum calculation
 * that supports different algorithms for different data formats (Lucene, Parquet, Arrow, etc.).</p>
 *
 * <h2>Architecture</h2>
 * <pre>
 *   ChecksumHandler (interface)
 *       ├── LuceneChecksumHandler     — CodecUtil footer read (O(1))
 *       ├── GenericCRC32ChecksumHandler — Full-file CRC32 scan (O(n), default fallback)
 *       └── [Plugin-provided handlers]  — Custom format-specific algorithms
 *
 *   ChecksumHandlerRegistry
 *       └── Maps format name → ChecksumHandler
 * </pre>
 *
 * <h2>Extension Points</h2>
 * <p>Plugins can provide custom checksum handlers by implementing
 * {@link org.opensearch.index.store.checksum.ChecksumHandler} and returning it from
 * {@link org.opensearch.plugins.DataSourcePlugin#getChecksumHandler()}.</p>
 *
 * <h2>Usage</h2>
 * <p>The {@link org.opensearch.index.store.checksum.ChecksumHandlerRegistry} is built by
 * {@link org.opensearch.index.store.DefaultCompositeStoreDirectoryFactory} from plugin-provided
 * handlers and injected into {@link org.opensearch.index.store.CompositeStoreDirectory}.</p>
 *
 * @opensearch.api
 */
package org.opensearch.index.store.checksum;
