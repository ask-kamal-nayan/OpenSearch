/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.iter;

import org.apache.lucene.util.LongsRef;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.parquet.bridge.NumericPageReader;
import org.opensearch.parquet.codec.cache.PageCache;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;

/**
 * Unit tests for the conditional read-time sort in {@link ParquetSortedNumericDocValues}, driven by
 * an in-memory {@link NumericPageReader} so no Parquet file or native layer is involved.
 *
 * <p>These are the backward-compatibility guards for the ingest-sort change: an UNMARKED file
 * ({@code valuesSorted == false}, i.e. a legacy or merged segment) MUST still be sorted on read,
 * while a MARKED file ({@code valuesSorted == true}) is served in its stored order.
 */
public class ParquetSortedNumericDocValuesTests extends OpenSearchTestCase {

    /** In-memory {@link NumericPageReader}: yields the exact longs registered for each row. */
    private static final class FakeNumericPageReader implements NumericPageReader {
        private final Map<Integer, long[]> rows;

        FakeNumericPageReader(Map<Integer, long[]> rows) {
            this.rows = rows;
        }

        @Override
        public PageCache cache() {
            return null;
        }

        @Override
        public void loadPageContaining(long row) {
            // no-op: everything is resident in the map
        }

        @Override
        public void readRepeatedLongsAtRow(long row, LongsRef dst) {
            long[] values = rows.getOrDefault((int) row, new long[0]);
            if (dst.longs.length < values.length) {
                dst.longs = new long[values.length];
            }
            System.arraycopy(values, 0, dst.longs, 0, values.length);
            dst.offset = 0;
            dst.length = values.length;
        }
    }

    public void testUnmarkedFileIsSortedOnRead() throws IOException {
        // Legacy/merged file: stored UNSORTED. The reader MUST sort so min/max stay correct.
        NumericPageReader reader = new FakeNumericPageReader(Map.of(0, new long[] { 8, 5, 8, 1, 3 }));
        ParquetSortedNumericDocValues dv = new ParquetSortedNumericDocValues(reader, 1, false);

        assertTrue(dv.advanceExact(0));
        assertEquals(5, dv.docValueCount());
        assertEquals(1L, dv.nextValue());
        assertEquals(3L, dv.nextValue());
        assertEquals(5L, dv.nextValue());
        assertEquals(8L, dv.nextValue());
        assertEquals(8L, dv.nextValue());
    }

    public void testMarkedFileIsServedInStoredOrder() throws IOException {
        // Marked file: stored ascending. The reader trusts the marker and does NOT re-sort.
        NumericPageReader reader = new FakeNumericPageReader(Map.of(0, new long[] { 1, 3, 5, 8, 8 }));
        ParquetSortedNumericDocValues dv = new ParquetSortedNumericDocValues(reader, 1, true);

        assertTrue(dv.advanceExact(0));
        assertEquals(5, dv.docValueCount());
        assertEquals(1L, dv.nextValue());
        assertEquals(3L, dv.nextValue());
        assertEquals(5L, dv.nextValue());
        assertEquals(8L, dv.nextValue());
        assertEquals(8L, dv.nextValue());
    }

    public void testMarkedAndUnmarkedSegmentsCoexist() throws IOException {
        // Two segments in the same index: one written before the change (unmarked, unsorted) and one
        // after (marked, pre-sorted). Each iterator honours its own file's marker.
        NumericPageReader legacy = new FakeNumericPageReader(Map.of(0, new long[] { 30, 10, 20 }));
        NumericPageReader current = new FakeNumericPageReader(Map.of(0, new long[] { 10, 20, 30 }));

        ParquetSortedNumericDocValues legacyDv = new ParquetSortedNumericDocValues(legacy, 1, false);
        ParquetSortedNumericDocValues currentDv = new ParquetSortedNumericDocValues(current, 1, true);

        assertTrue(legacyDv.advanceExact(0));
        assertEquals(10L, legacyDv.nextValue());
        assertEquals(20L, legacyDv.nextValue());
        assertEquals(30L, legacyDv.nextValue());

        assertTrue(currentDv.advanceExact(0));
        assertEquals(10L, currentDv.nextValue());
        assertEquals(20L, currentDv.nextValue());
        assertEquals(30L, currentDv.nextValue());
    }

    /**
     * Proves the ingest-side natural ordering of boxed doubles reproduces exactly the order the read
     * path compares in — Lucene's sortable-long encoding — for the tricky IEEE-754 values: NaN,
     * +0.0/-0.0, and the infinities. If these agreed only "mostly", a marked file would serve values
     * in the wrong order, so this is the correctness contract behind skipping the read-time sort.
     */
    public void testDoubleNaturalOrderMatchesSortableLongEncoding() throws IOException {
        double[] raw = { Double.NaN, 0.0d, -0.0d, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 3.14d, -2.5d };

        // Order produced at ingest: natural ordering of boxed Double (what ParquetField#writeList uses).
        Double[] ingestOrder = new Double[raw.length];
        for (int i = 0; i < raw.length; i++) {
            ingestOrder[i] = raw[i];
        }
        java.util.Arrays.sort(ingestOrder);

        // Order the read path sees: the stored values are Lucene sortable longs (previous stage), then
        // ParquetSortedNumericDocValues sorts those longs ascending. Feed the sortable-long encodings
        // through an unmarked iterator and read them back, decoding each to double for comparison.
        long[] encoded = new long[raw.length];
        for (int i = 0; i < raw.length; i++) {
            encoded[i] = NumericUtils.doubleToSortableLong(raw[i]);
        }
        NumericPageReader reader = new FakeNumericPageReader(Map.of(0, encoded));
        ParquetSortedNumericDocValues dv = new ParquetSortedNumericDocValues(reader, 1, false);
        assertTrue(dv.advanceExact(0));

        for (int i = 0; i < raw.length; i++) {
            double readOrdered = NumericUtils.sortableLongToDouble(dv.nextValue());
            double ingest = ingestOrder[i];
            // Compare by bits so NaN and -0.0/+0.0 are distinguished exactly.
            assertEquals(
                "position " + i + ": ingest natural order must match read sortable-long order",
                Double.doubleToLongBits(ingest),
                Double.doubleToLongBits(readOrdered)
            );
        }
    }
}
