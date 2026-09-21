/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues.iter;

import org.apache.lucene.search.DocIdSetIterator;
import org.opensearch.be.datafusion.docvalues.bridge.DecodedBatch;
import org.opensearch.be.datafusion.docvalues.bridge.DecodedListBatch;
import org.opensearch.be.datafusion.docvalues.bridge.ListValueReader;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;

/**
 * Drives {@link ParquetSortedNumericDocValues} over an in-memory {@link DecodedListBatch} built from
 * plain off-heap buffers, so the iterator's list extraction (W3) and read-side ascending sort (W12)
 * are exercised directly without the native cursor. The buffers are laid out exactly as the native
 * list cursor hands them over: an {@code i32} offsets buffer plus a flat child value buffer indexed
 * from child 0.
 */
public class ParquetSortedNumericDocValuesTests extends OpenSearchTestCase {

    /** GC-managed arena: the borrowed-buffer views stay valid for as long as the test holds them. */
    private final Arena arena = Arena.ofAuto();

    /** A multi-value row must surface every value through docValueCount()/nextValue(), not just the first. */
    public void testMultiValueRowReturnsAllValues() throws Exception {
        DecodedListBatch batch = listBatch(new long[] { 7, 2, 5 }, new long[] { 4, 1 });
        ParquetSortedNumericDocValues dv = new ParquetSortedNumericDocValues(reader(batch), 2, false);

        assertTrue(dv.advanceExact(0));
        assertEquals("doc0 must expose all three values, not one", 3, dv.docValueCount());
        assertEquals(2L, dv.nextValue());
        assertEquals(5L, dv.nextValue());
        assertEquals(7L, dv.nextValue());

        assertTrue(dv.advanceExact(1));
        assertEquals(2, dv.docValueCount());
        assertEquals(1L, dv.nextValue());
        assertEquals(4L, dv.nextValue());
    }

    /**
     * T7b: a file with NO values_sorted marker whose per-row values are in DESCENDING document order
     * must be returned ASCENDING, with min = first and max = last. This is the silent-correctness
     * guard: it fails if W12's read-side sort is reverted, and it fails if an absent marker
     * ({@code valuesSorted == false}) were wrongly treated as "already sorted".
     */
    public void testDescendingRowIsReturnedAscendingWhenMarkerAbsent() throws Exception {
        DecodedListBatch batch = listBatch(new long[] { 7, 2, 5 });
        // valuesSorted == false models an absent (or false) marker: the default-safe reader sorts.
        ParquetSortedNumericDocValues dv = new ParquetSortedNumericDocValues(reader(batch), 1, false);

        assertTrue(dv.advanceExact(0));
        assertEquals(3, dv.docValueCount());
        long first = dv.nextValue();
        long second = dv.nextValue();
        long third = dv.nextValue();
        assertEquals("min must be first", 2L, first);
        assertEquals(5L, second);
        assertEquals("max must be last", 7L, third);
        assertTrue("values must be strictly ascending", first < second && second < third);
    }

    /**
     * The marker only grants permission to SKIP the sort: with valuesSorted == true the iterator
     * trusts the writer and returns values in document order. This pins that the flag - not chance -
     * controls sorting, so the default-safe path in {@link #testDescendingRowIsReturnedAscendingWhenMarkerAbsent}
     * is genuinely the sort and not a no-op.
     */
    public void testMarkerTrueSkipsSortAndKeepsDocumentOrder() throws Exception {
        DecodedListBatch batch = listBatch(new long[] { 7, 2, 5 });
        ParquetSortedNumericDocValues dv = new ParquetSortedNumericDocValues(reader(batch), 1, true);

        assertTrue(dv.advanceExact(0));
        assertEquals(3, dv.docValueCount());
        assertEquals(7L, dv.nextValue());
        assertEquals(2L, dv.nextValue());
        assertEquals(5L, dv.nextValue());
    }

    /** An empty list means the document has no values, so advanceExact reports it absent. */
    public void testEmptyListDocumentIsAbsent() throws Exception {
        DecodedListBatch batch = listBatch(new long[] {}, new long[] { 4, 1 });
        ParquetSortedNumericDocValues dv = new ParquetSortedNumericDocValues(reader(batch), 2, false);

        assertFalse("an empty list is a document with no values", dv.advanceExact(0));
        assertTrue(dv.advanceExact(1));
        assertEquals(2, dv.docValueCount());
    }

    /** nextDoc/advance skip documents whose list is empty, landing on the next document that has values. */
    public void testNextDocSkipsEmptyRows() throws Exception {
        DecodedListBatch batch = listBatch(new long[] {}, new long[] { 9 }, new long[] {});
        ParquetSortedNumericDocValues dv = new ParquetSortedNumericDocValues(reader(batch), 3, false);

        assertEquals(1, dv.nextDoc());
        assertEquals(1, dv.docValueCount());
        assertEquals(9L, dv.nextValue());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());
    }

    /** A row with a single value still comes back as a one-element list, not a scalar. */
    public void testSingleValueRowStillCountsAsAList() throws Exception {
        DecodedListBatch batch = listBatch(new long[] { 42 });
        ParquetSortedNumericDocValues dv = new ParquetSortedNumericDocValues(reader(batch), 1, false);

        assertTrue(dv.advanceExact(0));
        assertEquals(1, dv.docValueCount());
        assertEquals(42L, dv.nextValue());
    }

    private static ListValueReader reader(DecodedListBatch batch) {
        return new ListValueReader() {
            @Override
            public DecodedListBatch decodedListBatch() {
                return batch;
            }

            @Override
            public void loadListBatchContaining(long row) {
                // The single resident batch already covers every row in these fixtures.
            }
        };
    }

    /**
     * Lays out {@code rows} exactly as the native list cursor exports them: an {@code i32} offsets
     * buffer of {@code rows.length + 1} entries over a flat {@code i64} child buffer indexed from
     * child 0, all values present.
     */
    private DecodedListBatch listBatch(long[]... rows) {
        int childCount = Arrays.stream(rows).mapToInt(r -> r.length).sum();
        MemorySegment offsets = arena.allocate((long) (rows.length + 1) * Integer.BYTES);
        MemorySegment values = arena.allocate((long) Math.max(childCount, 1) * Long.BYTES);
        int running = 0;
        int childIndex = 0;
        offsets.setAtIndex(ValueLayout.JAVA_INT, 0, 0);
        for (int r = 0; r < rows.length; r++) {
            for (long v : rows[r]) {
                values.setAtIndex(ValueLayout.JAVA_LONG, childIndex++, v);
            }
            running += rows[r].length;
            offsets.setAtIndex(ValueLayout.JAVA_INT, r + 1, running);
        }
        DecodedBatch child = new DecodedBatch(0, childCount - 1L, values, DecodedBatch.KIND_LONG, 0, null, 0);
        return new DecodedListBatch(0, rows.length - 1L, offsets, child);
    }
}
