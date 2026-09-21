/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues.bridge;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.be.datafusion.docvalues.iter.ParquetSortedNumericDocValues;
import org.opensearch.nativebridge.spi.ArrowExport;
import org.opensearch.parquet.bridge.NativeParquetWriter;
import org.opensearch.parquet.bridge.ParquetSortConfig;

import java.nio.file.Path;
import java.util.List;

/**
 * End-to-end coverage for the repeated (list) numeric doc-values read path: writes a real
 * {@code list<int64>} Parquet fixture with {@link NativeParquetWriter}, then reads each row's value
 * list back through {@link ParquetListColumnReader} and the FFM zero-copy borrow.
 */
public class ParquetListColumnReaderTests extends DataFusionBackedTestCase {

    private static final String COLUMN = "value";

    public void testReadsPerRowValueLists() throws Exception {
        long[][] rows = { { 7, 2, 5 }, { 4, 1 }, { 9 }, {} };
        Path file = createTempDir().resolve("lists.parquet");
        writeLongListColumn(file, rows);

        try (ParquetListColumnReader reader = ParquetListColumnReader.open(file, COLUMN)) {
            for (int row = 0; row < rows.length; row++) {
                reader.loadListBatchContaining(row);
                DecodedListBatch batch = reader.decodedListBatch();
                assertTrue("row " + row + " must be in the batch", batch.contains(row));
                int start = batch.startOffset(row);
                int end = batch.endOffset(row);
                assertEquals("row " + row + " list width", rows[row].length, end - start);
                DecodedBatch children = batch.childValues();
                for (int i = 0; i < rows[row].length; i++) {
                    assertTrue(children.isPresent(start + i));
                    assertEquals("row " + row + " value " + i, rows[row][i], children.valueAt(start + i));
                }
            }
        }
    }

    /**
     * Non-vacuous aggregation semantics over the full native read path plus the iterator's ascending
     * sort (W12): two docs on a long field, doc0 = [7, 2, 5] (descending, so the read-side sort must
     * run) and doc1 = [4, 1]. Reading ALL values gives sum = 19, count = 5, min = 1, max = 7,
     * avg = 3.8; a reader that returned only each row's FIRST value would see [7, 4] -> sum = 11,
     * count = 2, min = 4, avg = 5.5, so asserting every statistic together cannot pass vacuously.
     * The file carries no values_sorted marker, so the iterator is constructed unsorted (the
     * default-safe rule) and must sort each row itself. Falsifiable: reverting W12 leaves doc0 in
     * document order, breaking the per-doc ascending / min-first assertions; reverting the open-gate
     * fix makes {@code open} throw before any value is read.
     */
    public void testMultiValuedAggregationSemanticsThroughTheIterator() throws Exception {
        long[][] rows = { { 7, 2, 5 }, { 4, 1 } };
        Path file = createTempDir().resolve("agg-lists.parquet");
        writeLongListColumn(file, rows);

        try (ParquetListColumnReader reader = ParquetListColumnReader.open(file, COLUMN)) {
            // valuesSorted == false: no marker on this fixture, so the iterator must sort on read.
            ParquetSortedNumericDocValues dv = new ParquetSortedNumericDocValues(reader, rows.length, false);

            long sum = 0;
            long valueCount = 0;
            long min = Long.MAX_VALUE;
            long max = Long.MIN_VALUE;
            for (int doc = 0; doc < rows.length; doc++) {
                assertTrue("doc " + doc + " must have values", dv.advanceExact(doc));
                int count = dv.docValueCount();
                long previous = Long.MIN_VALUE;
                for (int i = 0; i < count; i++) {
                    long value = dv.nextValue();
                    assertTrue("doc " + doc + " values must be ascending (min first)", value >= previous);
                    previous = value;
                    sum += value;
                    valueCount++;
                    min = Math.min(min, value);
                    max = Math.max(max, value);
                }
            }

            // The fixed, non-vacuous expectations - a first-values-only reader ([7, 4]) cannot satisfy them.
            assertEquals("value_count over all list values", 5, valueCount);
            assertEquals("sum over all list values", 19, sum);
            assertEquals("min over all list values", 1, min);
            assertEquals("max over all list values", 7, max);
            assertEquals("avg over all list values", 3.8, (double) sum / valueCount, 0.0);
        }
    }

    private void writeLongListColumn(Path file, long[][] rows) throws Exception {
        try (ListVector listVector = ListVector.empty(COLUMN, allocator)) {
            UnionListWriter writer = listVector.getWriter();
            for (int r = 0; r < rows.length; r++) {
                writer.setPosition(r);
                writer.startList();
                for (long v : rows[r]) {
                    writer.bigInt().writeBigInt(v);
                }
                writer.endList();
            }
            writer.setValueCount(rows.length);

            Schema schema = new Schema(List.of(listVector.getField()));
            NativeParquetWriter parquetWriter = new NativeParquetWriter(file.toString());

            ArrowSchema schemaExport = ArrowSchema.allocateNew(allocator);
            Data.exportSchema(allocator, schema, null, schemaExport);
            try (ArrowExport export = new ArrowExport(null, schemaExport)) {
                parquetWriter.initialize("test-index", export.getSchemaAddress(), ParquetSortConfig.empty(), 0L);
            }

            try (VectorSchemaRoot root = new VectorSchemaRoot(schema.getFields(), List.of(listVector), rows.length)) {
                ArrowArray arrayExport = ArrowArray.allocateNew(allocator);
                ArrowSchema dataSchema = ArrowSchema.allocateNew(allocator);
                Data.exportVectorSchemaRoot(allocator, root, null, arrayExport, dataSchema);
                try (ArrowExport export = new ArrowExport(arrayExport, dataSchema)) {
                    parquetWriter.write(export.getArrayAddress(), export.getSchemaAddress());
                }
            }
            parquetWriter.flush();
        }
    }
}
