/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.checksum;

import org.apache.lucene.store.IndexInput;
import org.opensearch.common.annotation.PublicApi;

import java.io.IOException;

/**
 * Interface for format-specific checksum calculation.
 *
 * <p>Different data formats (Lucene, Parquet, Arrow, etc.) may use different
 * checksum algorithms and strategies. This interface allows each format to
 * provide its own checksum implementation through the plugin system.</p>
 *
 * <p>Built-in implementations:</p>
 * <ul>
 *   <li>{@link LuceneChecksumHandler} — reads checksum from Lucene codec footer (O(1))</li>
 *   <li>{@link GenericCRC32ChecksumHandler} — computes CRC32 over full file contents (O(n))</li>
 * </ul>
 *
 * <p>Plugins can provide custom implementations via {@code DataSourcePlugin.getChecksumHandler()}.</p>
 *
 * @opensearch.api
 */
@PublicApi(since = "3.0.0")
public interface ChecksumHandler {

    /**
     * Returns the format name this handler supports.
     *
     * <p>This must match the format name returned by {@code DataFormat.name()}
     * and used in {@code FileMetadata.dataFormat()}.</p>
     *
     * @return the format name (e.g., "lucene", "parquet", "arrow")
     */
    String getFormatName();

    /**
     * Calculates the checksum for the given IndexInput.
     *
     * <p>The implementation decides the algorithm:</p>
     * <ul>
     *   <li>Lucene: reads the codec footer checksum (fast, O(1))</li>
     *   <li>Generic: CRC32 over entire file contents (O(n))</li>
     *   <li>Custom: format-specific algorithm (e.g., Parquet footer checksum)</li>
     * </ul>
     *
     * @param input the IndexInput to calculate checksum from
     * @return the checksum value
     * @throws IOException if checksum calculation fails
     */
    long calculateChecksum(IndexInput input) throws IOException;

    /**
     * Calculates a checksum suitable for upload verification to remote store.
     *
     * <p>This may differ from {@link #calculateChecksum(IndexInput)} for certain formats.
     * For example, Lucene uses a "checksum-of-checksum" for upload verification.
     * The default implementation converts the regular checksum to a string.</p>
     *
     * @param input the IndexInput to calculate checksum from
     * @return checksum as a string representation suitable for upload verification
     * @throws IOException if checksum calculation fails
     */
    default String calculateUploadChecksum(IndexInput input) throws IOException {
        return Long.toString(calculateChecksum(input));
    }
}
