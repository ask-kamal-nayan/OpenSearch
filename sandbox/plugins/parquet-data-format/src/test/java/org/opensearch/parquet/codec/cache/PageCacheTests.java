/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.cache;

import org.apache.lucene.util.NumericUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.foreign.MemorySegment;

/**
 * Unit tests for {@link PageCache} (task 3.4): presence bit-test correctness across word
 * boundaries and primitive value lookup by global row. Values/presence are off-heap views;
 * tests back them with heap-array segments ({@link MemorySegment#ofArray}), which exercise
 * the same accessor paths.
 */
public class PageCacheTests extends OpenSearchTestCase {

    public void testPresenceBitTestWithinAndAcrossWords() {
        PageCache pc = new PageCache();
        pc.firstRow = 100;
        pc.lastRow = 199; // 100 rows
        long[] values = new long[100];
        // Mark even local indices present.
        long[] presenceBits = new long[(100 + 63) >> 6];
        for (int i = 0; i < 100; i++) {
            if (i % 2 == 0) {
                presenceBits[i >> 6] |= (1L << (i & 63));
            }
            values[i] = 1000 + i;
        }
        pc.values = MemorySegment.ofArray(values);
        pc.presenceBits = MemorySegment.ofArray(presenceBits);

        assertEquals(100, pc.rowCount());
        for (long row = 100; row <= 199; row++) {
            int local = (int) (row - 100);
            boolean expectedPresent = local % 2 == 0;
            assertEquals("row " + row, expectedPresent, pc.isPresent(row));
            assertEquals("value at row " + row, 1000L + local, pc.valueAt(row));
        }
    }

    public void testPresenceAcrossWord64Boundary() {
        PageCache pc = new PageCache();
        pc.firstRow = 0;
        pc.lastRow = 127; // exactly two 64-bit words
        long[] presenceBits = new long[2];
        // Set local indices 63 and 64 (the word boundary).
        presenceBits[63 >> 6] |= (1L << (63 & 63));
        presenceBits[64 >> 6] |= (1L << (64 & 63));
        pc.presenceBits = MemorySegment.ofArray(presenceBits);

        assertFalse(pc.isPresent(62));
        assertTrue(pc.isPresent(63));
        assertTrue(pc.isPresent(64));
        assertFalse(pc.isPresent(65));
    }

    public void testContains() {
        PageCache pc = new PageCache();
        pc.firstRow = 10;
        pc.lastRow = 20;
        assertFalse(pc.contains(9));
        assertTrue(pc.contains(10));
        assertTrue(pc.contains(15));
        assertTrue(pc.contains(20));
        assertFalse(pc.contains(21));
    }

    // Ordered strictly ascending numerically (NaN handled separately) covering negatives, mixed
    // sign, -0.0/+0.0 and -inf/+inf. Shared by the borrowed and copied double/float cases below.
    private static final double[] ORDERED_DOUBLES = {
        Double.NEGATIVE_INFINITY, -3.0, -1.5, -0.5, -0.0, 0.0, 0.5, 1.5, 3.0, Double.POSITIVE_INFINITY };
    private static final float[] ORDERED_FLOATS = {
        Float.NEGATIVE_INFINITY, -3.0f, -1.5f, -0.5f, -0.0f, 0.0f, 0.5f, 1.5f, 3.0f, Float.POSITIVE_INFINITY };

    /**
     * Borrowed double route: Arrow memory holds raw f64 bits read in place, so {@link
     * PageCache#valueAt} applies the sortable transform. Reading back through OpenSearch's
     * {@code sortableLongToDouble} must recover the original value and ascending long order must
     * match ascending numeric order.
     */
    public void testBorrowedDoubleValueAtDecodesToOriginalAndPreservesOrder() {
        long[] rawBits = new long[ORDERED_DOUBLES.length];
        for (int i = 0; i < ORDERED_DOUBLES.length; i++) {
            rawBits[i] = Double.doubleToRawLongBits(ORDERED_DOUBLES[i]);
        }
        PageCache pc = new PageCache();
        pc.firstRow = 0;
        pc.lastRow = ORDERED_DOUBLES.length - 1;
        pc.valueKind = PageCache.KIND_DOUBLE;
        pc.values = MemorySegment.ofArray(rawBits);

        long prev = Long.MIN_VALUE;
        for (int i = 0; i < ORDERED_DOUBLES.length; i++) {
            long sortable = pc.valueAt(i);
            assertEquals("decode double at " + i, ORDERED_DOUBLES[i], NumericUtils.sortableLongToDouble(sortable), 0.0);
            assertTrue("ascending order at " + i, sortable > prev);
            prev = sortable;
        }

        // NaN round-trips and sorts above +inf, matching Double#compareTo.
        long[] nan = { Double.doubleToRawLongBits(Double.NaN) };
        pc.values = MemorySegment.ofArray(nan);
        pc.firstRow = 0;
        pc.lastRow = 0;
        assertTrue(Double.isNaN(NumericUtils.sortableLongToDouble(pc.valueAt(0))));
        assertTrue(pc.valueAt(0) > prev);
    }

    /**
     * Borrowed float route: Arrow memory holds raw f32 bits, read as 4-byte ints (the width the
     * borrow reinterpret must supply) and transformed to a sign-extended sortable value by {@link
     * PageCache#valueAt}. {@code sortableIntToFloat} must recover the original and order must hold.
     */
    public void testBorrowedFloatValueAtDecodesToOriginalAndPreservesOrder() {
        int[] rawBits = new int[ORDERED_FLOATS.length];
        for (int i = 0; i < ORDERED_FLOATS.length; i++) {
            rawBits[i] = Float.floatToRawIntBits(ORDERED_FLOATS[i]);
        }
        PageCache pc = new PageCache();
        pc.firstRow = 0;
        pc.lastRow = ORDERED_FLOATS.length - 1;
        pc.valueKind = PageCache.KIND_FLOAT;
        pc.values = MemorySegment.ofArray(rawBits);

        long prev = Long.MIN_VALUE;
        for (int i = 0; i < ORDERED_FLOATS.length; i++) {
            long sortable = pc.valueAt(i);
            assertEquals("decode float at " + i, ORDERED_FLOATS[i], NumericUtils.sortableIntToFloat((int) sortable), 0.0f);
            assertTrue("ascending order at " + i, sortable > prev);
            prev = sortable;
        }

        int[] nan = { Float.floatToRawIntBits(Float.NaN) };
        pc.values = MemorySegment.ofArray(nan);
        pc.firstRow = 0;
        pc.lastRow = 0;
        assertTrue(Float.isNaN(NumericUtils.sortableIntToFloat((int) pc.valueAt(0))));
        assertTrue(pc.valueAt(0) > prev);
    }

    /**
     * Copied route: Rust owns the sortable transform on the copy paths, so a copy-mode page (default
     * {@code KIND_LONG}) already holds sortable longs and {@link PageCache#valueAt} must return them
     * verbatim - no second Java transform. Verified for both float (widened sortable int) and double.
     */
    public void testCopiedSortableLongsPassThroughForDoubleAndFloat() {
        long[] sortableDoubles = new long[ORDERED_DOUBLES.length];
        for (int i = 0; i < ORDERED_DOUBLES.length; i++) {
            sortableDoubles[i] = NumericUtils.doubleToSortableLong(ORDERED_DOUBLES[i]);
        }
        PageCache pc = new PageCache();
        pc.firstRow = 0;
        pc.lastRow = ORDERED_DOUBLES.length - 1;
        // Copy mode leaves valueKind at its KIND_LONG default: raw long passthrough.
        pc.values = MemorySegment.ofArray(sortableDoubles);
        long prev = Long.MIN_VALUE;
        for (int i = 0; i < ORDERED_DOUBLES.length; i++) {
            long got = pc.valueAt(i);
            assertEquals("passthrough double at " + i, sortableDoubles[i], got);
            assertEquals(ORDERED_DOUBLES[i], NumericUtils.sortableLongToDouble(got), 0.0);
            assertTrue(got > prev);
            prev = got;
        }

        // Float copy path widens each sortable int into an 8-byte slot (sign-extended), exactly what
        // the Rust FLOAT arm writes; passthrough must preserve it for sortableIntToFloat.
        long[] sortableFloats = new long[ORDERED_FLOATS.length];
        for (int i = 0; i < ORDERED_FLOATS.length; i++) {
            sortableFloats[i] = NumericUtils.floatToSortableInt(ORDERED_FLOATS[i]);
        }
        pc.values = MemorySegment.ofArray(sortableFloats);
        pc.lastRow = ORDERED_FLOATS.length - 1;
        prev = Long.MIN_VALUE;
        for (int i = 0; i < ORDERED_FLOATS.length; i++) {
            long got = pc.valueAt(i);
            assertEquals("passthrough float at " + i, sortableFloats[i], got);
            assertEquals(ORDERED_FLOATS[i], NumericUtils.sortableIntToFloat((int) got), 0.0f);
            assertTrue(got > prev);
            prev = got;
        }
    }
}
