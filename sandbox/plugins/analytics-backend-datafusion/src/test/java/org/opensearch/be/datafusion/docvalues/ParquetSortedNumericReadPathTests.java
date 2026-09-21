/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.opensearch.be.datafusion.docvalues.bridge.DataFusionBackedTestCase;
import org.opensearch.be.datafusion.docvalues.bridge.ParquetColumnReader;
import org.opensearch.be.datafusion.docvalues.bridge.ParquetListColumnReader;
import org.opensearch.be.datafusion.docvalues.iter.ParquetSortedNumericDocValues;
import org.opensearch.common.settings.Settings;
import org.opensearch.nativebridge.spi.ArrowExport;
import org.opensearch.parquet.bridge.NativeParquetWriter;
import org.opensearch.parquet.bridge.ParquetSortConfig;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * End-to-end read-path coverage for the multi-valued numeric doc-values sort, exercised over a
 * <em>real</em> Parquet LIST file across the full native FFM boundary rather than the hand-built
 * off-heap buffers of {@link org.opensearch.be.datafusion.docvalues.iter.ParquetSortedNumericDocValuesTests}.
 *
 * <p>The headline property of this feature is that {@link SortedNumericDocValues} must expose each
 * document's values ascending ({@code min == first}, {@code max == last}) even when the values were
 * stored on disk in some other order and the file carries no {@code opensearch.values_sorted} marker.
 * A file written with the ingest sort off (the default) stamps no marker, so
 * {@link ParquetDocValuesProducer} reads {@code valuesSorted == false} from the footer and the
 * iterator must sort each row on read. This is the only guard against a Lucene {@code min}/{@code max}
 * aggregator returning the first stored element instead of the true minimum.
 *
 * <p>These cases drive the real {@link ParquetDocValuesProducer#getSortedNumeric} over a
 * {@link NativeParquetWriter}-written LIST column so the values arrive through the native list cursor
 * ({@link ParquetListColumnReader}) exactly as they do under a live shard. The producer's own
 * javadoc-documented topology (a Lucene aggregator over the wrapped reader) cannot be stood up in a
 * unit test - and a core {@code _search} over a pluggable-Parquet shard is refused by the
 * {@code DataFormatAwareEngine} before a Lucene searcher is even acquired - so the producer read is
 * the highest-fidelity boundary at which the on-disk-order-to-ascending contract is observable.
 */
public class ParquetSortedNumericReadPathTests extends DataFusionBackedTestCase {

    private static final String COLUMN = "vals";

    /**
     * An unmarked file whose per-row value lists are stored in strictly DESCENDING document order must
     * be returned ASCENDING, so {@code min} is the true minimum and {@code max} the true maximum - not
     * the first stored element. Read through the real producer, so it also confirms the producer reads
     * the absent marker as {@code valuesSorted == false} and therefore takes the sorting path.
     *
     * <p>Falsifiable in two independent directions: it fails with {@code min == 9} if the read-side
     * {@code Arrays.sort} in {@link ParquetSortedNumericDocValues} is removed, and it fails if an absent
     * marker were wrongly treated as "already sorted" (the sort would be skipped and the descending
     * on-disk order would leak through).
     */
    public void testUnmarkedDescendingListIsReturnedAscendingSoMinIsTrueMinimum() throws Exception {
        long[][] rows = { { 9, 5, 1 }, { 8, 4, 2 } };
        Path file = createTempDir().resolve("unmarked-descending.parquet");
        writeLongListColumn(file, rows);

        FieldInfo declaredMultiValued = fieldInfo(COLUMN, 0, DocValuesType.SORTED_NUMERIC);
        try (
            FSDirectory directory = FSDirectory.open(file.getParent());
            ParquetDocValuesProducer producer = new ParquetDocValuesProducer(
                segmentReadState(directory, file, rows.length, declaredMultiValued),
                null
            );
            CursorRegistry cursors = new CursorRegistry()
        ) {
            SortedNumericDocValues dv = producer.getSortedNumeric(declaredMultiValued, cursors);

            assertTrue("doc 0 must be present", dv.advanceExact(0));
            assertEquals("every value must survive, not just the first", 3, dv.docValueCount());
            long first0 = dv.nextValue();
            long second0 = dv.nextValue();
            long third0 = dv.nextValue();
            assertEquals("min must be the TRUE minimum (1), not the first stored element (9)", 1L, first0);
            assertEquals(5L, second0);
            assertEquals("max must be the TRUE maximum (9), the last value after the read-side sort", 9L, third0);
            assertTrue("row 0 must be strictly ascending", first0 < second0 && second0 < third0);

            assertTrue("doc 1 must be present", dv.advanceExact(1));
            assertEquals(3, dv.docValueCount());
            assertEquals("min of the second row must be its true minimum (2), not the first stored element (8)", 2L, dv.nextValue());
            assertEquals(4L, dv.nextValue());
            assertEquals(8L, dv.nextValue());
        }
    }

    /**
     * The mirror direction: with {@code valuesSorted == true} - the state the producer memoizes when a
     * present-and-true {@code opensearch.values_sorted} marker is read - the iterator must TRUST the
     * writer and return each row in its on-disk order without re-sorting. Driven over the same real
     * descending list cursor as the unmarked case above, so the only difference is the flag; the
     * descending values come back unchanged, proving the flag - not chance - controls the sort.
     *
     * <p>This pins the branch in both directions: the unmarked test proves the reader sorts, this proves
     * the marker genuinely skips that sort. It is unaffected by removing the read-side {@code Arrays.sort}
     * (it never sorts), which is why it stays green during the falsifiability demonstration while the
     * unmarked test flips.
     */
    public void testValuesSortedMarkerTrustsWriterOrderAndSkipsSort() throws Exception {
        long[][] rows = { { 9, 5, 1 } };
        Path file = createTempDir().resolve("descending-list.parquet");
        writeLongListColumn(file, rows);

        try (
            CursorRegistry cursors = new CursorRegistry();
            ParquetListColumnReader listReader = ParquetListColumnReader.open(file, COLUMN, Settings.EMPTY, ParquetColumnReader.LOCAL_STORE)
        ) {
            cursors.register(listReader);
            // valuesSorted == true models a present-and-true marker: the reader trusts on-disk order.
            SortedNumericDocValues dv = new ParquetSortedNumericDocValues(listReader, rows.length, true);

            assertTrue(dv.advanceExact(0));
            assertEquals(3, dv.docValueCount());
            assertEquals("marker=true must skip the sort and keep the writer's order", 9L, dv.nextValue());
            assertEquals(5L, dv.nextValue());
            assertEquals(1L, dv.nextValue());
        }
    }

    /**
     * Writes {@code rows} as a real {@code list<int64>} Parquet column through {@link NativeParquetWriter},
     * with {@link ParquetSortConfig#empty()} so the ingest sort is off and no {@code opensearch.values_sorted}
     * marker is stamped - the file's on-disk value order is exactly the order given here.
     */
    private void writeLongListColumn(Path file, long[][] rows) throws Exception {
        try (ListVector listVector = ListVector.empty(COLUMN, allocator)) {
            UnionListWriter listWriter = listVector.getWriter();
            for (int r = 0; r < rows.length; r++) {
                listWriter.setPosition(r);
                listWriter.startList();
                for (long v : rows[r]) {
                    listWriter.bigInt().writeBigInt(v);
                }
                listWriter.endList();
            }
            listWriter.setValueCount(rows.length);

            Schema schema = new Schema(List.of(listVector.getField()));
            NativeParquetWriter parquetWriter = new NativeParquetWriter(file.toString());

            ArrowSchema schemaExport = ArrowSchema.allocateNew(allocator);
            Data.exportSchema(allocator, schema, null, schemaExport);
            try (ArrowExport export = new ArrowExport(null, schemaExport)) {
                parquetWriter.initialize("test-index", export.getSchemaAddress(), ParquetSortConfig.empty(), 0L, false);
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

    private static SegmentReadState segmentReadState(FSDirectory directory, Path parquetFile, int maxDoc, FieldInfo... fields) {
        SegmentInfo segmentInfo = new SegmentInfo(
            directory,
            Version.LATEST,
            Version.LATEST,
            "_0",
            maxDoc,
            false,
            false,
            Codec.getDefault(),
            Map.of(),
            StringHelper.randomId(),
            Map.of(ParquetSegmentLayout.PARQUET_FILE_ATTRIBUTE, parquetFile.toString()),
            null
        );
        return new SegmentReadState(directory, segmentInfo, new FieldInfos(fields), IOContext.DEFAULT);
    }

    private static FieldInfo fieldInfo(String name, int number, DocValuesType docValuesType) {
        return new FieldInfo(
            name,
            number,
            false,
            false,
            true,
            IndexOptions.NONE,
            docValuesType,
            DocValuesSkipIndexType.NONE,
            -1,
            Map.of(),
            0,
            0,
            0,
            0,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,
            false
        );
    }
}
