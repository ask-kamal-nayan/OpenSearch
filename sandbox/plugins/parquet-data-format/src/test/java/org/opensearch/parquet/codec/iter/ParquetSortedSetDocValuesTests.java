/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.iter;

import org.apache.lucene.util.BytesRef;
import org.opensearch.parquet.bridge.BinaryPageReader;
import org.opensearch.parquet.codec.cache.PageCache;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Unit tests for the conditional read-time sort in {@link ParquetSortedSetDocValues}, driven by an
 * in-memory {@link BinaryPageReader}.
 *
 * <p>Backward-compatibility guards: an UNMARKED file ({@code valuesSorted == false}) MUST still be
 * sorted on read; a MARKED file ({@code valuesSorted == true}) skips the sort. In BOTH cases
 * SORTED_SET de-duplication still runs — de-dup does not depend on the sort being performed here,
 * only on equal values being adjacent (which the ingest sort guarantees for marked files).
 */
public class ParquetSortedSetDocValuesTests extends OpenSearchTestCase {

    /** In-memory {@link BinaryPageReader}: yields the exact byte rows registered for each row. */
    private static final class FakeBinaryPageReader implements BinaryPageReader {
        private final Map<Integer, byte[][]> rows;

        FakeBinaryPageReader(Map<Integer, byte[][]> rows) {
            this.rows = rows;
        }

        @Override
        public PageCache cache() {
            return null;
        }

        @Override
        public void loadPageContaining(long row) {
            // no-op
        }

        @Override
        public byte[][] readRepeatedBytesAtRow(long row) {
            return rows.getOrDefault((int) row, new byte[0][]);
        }
    }

    private static byte[][] bytes(String... values) {
        byte[][] out = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            out[i] = values[i].getBytes(StandardCharsets.UTF_8);
        }
        return out;
    }

    public void testUnmarkedFileIsSortedAndDedupedOnRead() throws IOException {
        // Legacy file: stored UNSORTED with a duplicate. Reader must sort AND de-duplicate.
        BinaryPageReader reader = new FakeBinaryPageReader(Map.of(0, bytes("omega", "alpha", "omega", "beta")));
        ParquetSortedSetDocValues dv = new ParquetSortedSetDocValues(reader, true, 1, false);

        assertTrue(dv.advanceExact(0));
        assertEquals(3, dv.docValueCount());
        assertEquals(new BytesRef("alpha"), dv.lookupOrd(dv.nextOrd()));
        assertEquals(new BytesRef("beta"), dv.lookupOrd(dv.nextOrd()));
        assertEquals(new BytesRef("omega"), dv.lookupOrd(dv.nextOrd()));
    }

    public void testMarkedFileSkipsSortButStillDedups() throws IOException {
        // Marked file: stored pre-sorted (UTF-8 order) with the duplicate adjacent. Sort is skipped
        // but the adjacent duplicate is still collapsed.
        BinaryPageReader reader = new FakeBinaryPageReader(Map.of(0, bytes("alpha", "beta", "omega", "omega")));
        ParquetSortedSetDocValues dv = new ParquetSortedSetDocValues(reader, true, 1, true);

        assertTrue(dv.advanceExact(0));
        assertEquals(3, dv.docValueCount());
        assertEquals(new BytesRef("alpha"), dv.lookupOrd(dv.nextOrd()));
        assertEquals(new BytesRef("beta"), dv.lookupOrd(dv.nextOrd()));
        assertEquals(new BytesRef("omega"), dv.lookupOrd(dv.nextOrd()));
    }
}
