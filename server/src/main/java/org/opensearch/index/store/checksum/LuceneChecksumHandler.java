/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.checksum;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.annotation.PublicApi;

import java.io.IOException;

/**
 * Checksum handler for Lucene format files.
 *
 * <p>Lucene files embed a CRC32 checksum in a "codec footer" at the end of every file.
 * This handler reads only the footer (last 8 bytes), making it O(1) regardless of file size.</p>
 *
 * @opensearch.api
 */
@PublicApi(since = "3.0.0")
public class LuceneChecksumHandler implements ChecksumHandler {

    public static final String FORMAT_NAME = "lucene";

    @Override
    public String getFormatName() {
        return FORMAT_NAME;
    }

    /**
     * Retrieves the checksum from the Lucene codec footer.
     *
     * <p>This is an O(1) operation that reads only the last 8 bytes of the file,
     * where Lucene stores the pre-computed CRC32 checksum.</p>
     *
     * @param input the IndexInput to read the codec footer from
     * @return the checksum value from the Lucene codec footer
     * @throws IOException if the file cannot be read or has no valid codec footer
     */
    @Override
    public long calculateChecksum(IndexInput input) throws IOException {
        return CodecUtil.retrieveChecksum(input);
    }
}
