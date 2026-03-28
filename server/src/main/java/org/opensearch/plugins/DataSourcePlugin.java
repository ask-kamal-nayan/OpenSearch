/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.IndexingExecutionEngine;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.checksum.ChecksumHandler;
import org.opensearch.index.store.checksum.GenericCRC32ChecksumHandler;
import org.opensearch.plugins.spi.vectorized.DataSourceCodec;
import org.opensearch.index.store.FormatStoreDirectory;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public interface DataSourcePlugin {
    default Optional<Map<org.opensearch.plugins.spi.vectorized.DataFormat, DataSourceCodec>> getDataSourceCodecs() {
        return Optional.empty();
    }

    <T extends DataFormat> IndexingExecutionEngine<T> indexingEngine(MapperService mapperService, ShardPath shardPath, IndexSettings indexSettings);

    FormatStoreDirectory<?> createFormatStoreDirectory(
        IndexSettings indexSettings,
        ShardPath shardPath
    ) throws IOException;

    BlobContainer createBlobContainer(BlobStore blobStore, BlobPath blobPath) throws IOException;

    DataFormat getDataFormat();

    /**
     * Provides a format-specific checksum handler for this data format plugin.
     *
     * <p>If not overridden, a {@link GenericCRC32ChecksumHandler} is returned that
     * computes CRC32 over the full file contents. Plugins can override this to provide
     * format-specific checksum strategies (e.g., reading checksum from Parquet footer).</p>
     *
     * <p>The returned handler's format name must match {@code getDataFormat().name()}.</p>
     *
     * @return a ChecksumHandler for this format's files
     */
    default ChecksumHandler getChecksumHandler() {
        return new GenericCRC32ChecksumHandler(getDataFormat().name());
    }
}
