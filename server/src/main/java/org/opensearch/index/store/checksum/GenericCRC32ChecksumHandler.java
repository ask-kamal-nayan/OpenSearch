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
import java.util.zip.CRC32;

/**
 * Generic checksum handler that computes CRC32 over the entire file contents.
 *
 * <p>This handler is used as the default/fallback for any data format that does not
 * provide its own {@link ChecksumHandler} implementation. It works for any file format
 * (Parquet, Arrow, ORC, etc.) but is O(n) in file size because it reads every byte.</p>
 *
 * <p>Plugins can override this by providing a custom {@link ChecksumHandler} via
 * {@code DataSourcePlugin.getChecksumHandler()} — for example, a Parquet plugin
 * might read the checksum from the Parquet footer instead of scanning the full file.</p>
 *
 * @opensearch.api
 */
@PublicApi(since = "3.0.0")
public class GenericCRC32ChecksumHandler implements ChecksumHandler {

    /**
     * Buffer size for reading file contents during checksum calculation.
     */
    private static final int CHECKSUM_BUFFER_SIZE = 8192;

    private final String formatName;

    /**
     * Creates a GenericCRC32ChecksumHandler with format name "generic".
     * Used as the default fallback handler.
     */
    public GenericCRC32ChecksumHandler() {
        this.formatName = "generic";
    }

    /**
     * Creates a GenericCRC32ChecksumHandler for a specific format.
     * Used when a format plugin does not provide its own handler but
     * needs to be registered with its format name.
     *
     * @param formatName the format name (e.g., "parquet", "arrow")
     */
    public GenericCRC32ChecksumHandler(String formatName) {
        this.formatName = formatName;
    }

    @Override
    public String getFormatName() {
        return formatName;
    }

    /**
     * Calculates a CRC32 checksum over the entire file contents.
     *
     * <p>This is an O(n) operation that reads every byte of the file.
     * It is generic and works for any file format.</p>
     *
     * @param input the IndexInput to compute the checksum from
     * @return the CRC32 checksum value
     * @throws IOException if the file cannot be read
     */
    @Override
    public long calculateChecksum(IndexInput input) throws IOException {
        CRC32 crc32 = new CRC32();
        byte[] buffer = new byte[CHECKSUM_BUFFER_SIZE];
        long remaining = input.length();

        while (remaining > 0) {
            int toRead = (int) Math.min(buffer.length, remaining);
            input.readBytes(buffer, 0, toRead);
            crc32.update(buffer, 0, toRead);
            remaining -= toRead;
        }

        return crc32.getValue();
    }
}
