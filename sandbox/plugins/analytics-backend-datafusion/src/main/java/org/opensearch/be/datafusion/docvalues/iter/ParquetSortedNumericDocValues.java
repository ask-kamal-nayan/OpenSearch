/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues.iter;

import org.apache.lucene.index.SortedNumericDocValues;
import org.opensearch.be.datafusion.docvalues.bridge.DecodedBatch;
import org.opensearch.be.datafusion.docvalues.bridge.DecodedListBatch;
import org.opensearch.be.datafusion.docvalues.bridge.ListValueReader;

import java.io.IOException;
import java.util.Arrays;

/**
 * {@link SortedNumericDocValues} over a repeated (multi-valued) Parquet primitive column.
 *
 * <p>Each document maps to exactly one Parquet row whose value list is carved out of the reader's
 * resident {@link DecodedListBatch} using its per-row offsets. When the requested document falls
 * outside that batch, {@link ListValueReader#loadListBatchContaining} decodes the batch that holds
 * it (the only step that crosses the native boundary). Float and double values arrive already
 * re-encoded to their Lucene-sortable form by {@link DecodedBatch#valueAt}.
 *
 * <p>Lucene's {@code SortedNumericDocValues} contract requires each document's values in ascending
 * order ({@code docValueCount()} then {@code nextValue()} ascending; min = first, max = last), so
 * the extracted list is sorted on read. The sort is skipped only when {@code valuesSorted} is true,
 * i.e. the writer stamped the {@code opensearch.values_sorted} marker: an absent or false marker -
 * every file written before that feature, and every file written with the ingest sort off - means
 * ingest order cannot be trusted, so the reader sorts. Correctness never depends on the marker; it
 * only grants permission to skip the sort.
 */
public final class ParquetSortedNumericDocValues extends SortedNumericDocValues {

    private final ListValueReader reader;
    private final int maxDoc;
    private final boolean valuesSorted;

    private int doc = -1;
    /** Reused per-document value buffer, grown as needed; only {@code [0, count)} is live. */
    private long[] values = new long[8];
    private int count;
    private int cursor;

    /**
     * @param valuesSorted whether the writer proved each row's values already ascending (the
     *                     {@code opensearch.values_sorted} marker read true); false defaults the
     *                     reader to sorting, which is correct for every file
     */
    public ParquetSortedNumericDocValues(ListValueReader reader, int maxDoc, boolean valuesSorted) {
        this.reader = reader;
        this.maxDoc = maxDoc;
        this.valuesSorted = valuesSorted;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        if (target >= maxDoc) {
            doc = NO_MORE_DOCS;
            return false;
        }
        doc = target;
        DecodedListBatch batch = reader.decodedListBatch();
        if (batch == null || batch.contains(target) == false) {
            reader.loadListBatchContaining(target);
            batch = reader.decodedListBatch();
        }
        int start = batch.startOffset(target);
        int end = batch.endOffset(target);
        DecodedBatch children = batch.childValues();

        ensureCapacity(end - start);
        int n = 0;
        for (int child = start; child < end; child++) {
            // Child-level nulls are dropped: SortedNumericDocValues has no per-value absence, only
            // per-document, so a null element simply is not one of the document's values.
            if (children.isPresent(child)) {
                values[n++] = children.valueAt(child);
            }
        }
        count = n;
        cursor = 0;
        if (count == 0) {
            // A null or empty list means the document has no values. SortedNumericDocValues
            // represents that as advanceExact == false (the document is absent from the iterator),
            // which is why a missing list-level null bitmap does not matter here: null list and
            // empty list are indistinguishable to this contract and both mean "no values".
            return false;
        }
        // Default-safe ascending sort (see class javadoc): sort unless the writer marked the values
        // already sorted. Skipping this on an unmarked file would return min/max in the wrong order.
        if (valuesSorted == false) {
            Arrays.sort(values, 0, count);
        }
        return true;
    }

    @Override
    public int docValueCount() {
        return count;
    }

    @Override
    public long nextValue() {
        return values[cursor++];
    }

    @Override
    public int docID() {
        return doc;
    }

    @Override
    public int nextDoc() throws IOException {
        if (doc == NO_MORE_DOCS) {
            return NO_MORE_DOCS;
        }
        return advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
        for (int d = target; d < maxDoc; d++) {
            if (advanceExact(d)) {
                doc = d;
                return d;
            }
        }
        doc = NO_MORE_DOCS;
        return NO_MORE_DOCS;
    }

    @Override
    public long cost() {
        return maxDoc;
    }

    private void ensureCapacity(int needed) {
        if (needed > values.length) {
            values = new long[Math.max(needed, values.length * 2)];
        }
    }
}
