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
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
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
import org.opensearch.nativebridge.spi.ArrowExport;
import org.opensearch.parquet.bridge.NativeParquetWriter;
import org.opensearch.parquet.bridge.ParquetSortConfig;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Covers the producer's physical-shape probe: {@link ParquetDocValuesProducer#isRepeated} reads the
 * column's shape from the Parquet file itself, and {@code getSortedNumeric} routes a field to the
 * list reader only when the mapping declares it multi-valued AND the file is physically a LIST. The
 * two signals can legitimately disagree after a scalar-to-LIST promotion, so an older, physically
 * scalar segment of a now-multi-valued field must still read through the single-valued path instead
 * of failing the native list downcast.
 *
 * <p>Real {@code int64} and {@code list<int64>} fixtures are written with {@link NativeParquetWriter}
 * so the probe crosses the full FFM boundary, and every field below is declared
 * {@code SORTED_NUMERIC} - the DV type the leaf reader stamps on a mapping-multi-valued field - so a
 * probe that consulted the declared type rather than the file would report both shapes as repeated.
 */
public class ParquetDocValuesProducerPhysicalShapeTests extends DataFusionBackedTestCase {

    private static final String COLUMN = "value";

    /**
     * A physically repeated ({@code list<int64>}) column reports repeated, so a multi-valued-declared
     * field over it takes the list path. Falsifiable: forcing {@code isRepeated} to {@code false} (or
     * the native probe to report scalar) flips this.
     */
    public void testPhysicallyRepeatedColumnReportsRepeated() throws Exception {
        long[][] rows = { { 7, 2, 5 }, { 4, 1 }, { 9 }, {} };
        Path file = createTempDir().resolve("repeated-shape.parquet");
        writeLongListColumn(file, rows);

        FieldInfo declaredMultiValued = fieldInfo(COLUMN, 0, DocValuesType.SORTED_NUMERIC);
        try (
            FSDirectory directory = FSDirectory.open(file.getParent());
            ParquetDocValuesProducer producer = new ParquetDocValuesProducer(
                segmentReadState(directory, file, rows.length, declaredMultiValued),
                null
            )
        ) {
            assertTrue("a physically LIST column must report repeated", producer.isRepeated(declaredMultiValued));
            // Memoized: a second call must agree with the first.
            assertTrue(producer.isRepeated(declaredMultiValued));
        }
    }

    /**
     * A physically scalar column reports NOT repeated even though its mapping declares it
     * multi-valued (the field carries {@code SORTED_NUMERIC}). This is the disagreement case a
     * scalar-to-LIST promotion produces; mirrors the tested branch's
     * {@code assertFalse("scalar column must not report repeated despite a SORTED_NUMERIC dv type")}.
     * Falsifiable: routing the probe on the declared type would make this report repeated.
     */
    public void testPhysicallyScalarColumnReportsNotRepeatedDespiteMultiValuedDeclaration() throws Exception {
        long[] values = { 11, 22, 33, 44 };
        Path file = createTempDir().resolve("scalar-shape.parquet");
        writeLongScalarColumn(file, values);

        FieldInfo declaredMultiValued = fieldInfo(COLUMN, 0, DocValuesType.SORTED_NUMERIC);
        try (
            FSDirectory directory = FSDirectory.open(file.getParent());
            ParquetDocValuesProducer producer = new ParquetDocValuesProducer(
                segmentReadState(directory, file, values.length, declaredMultiValued),
                null
            )
        ) {
            assertFalse(
                "a physically scalar column must not report repeated despite a SORTED_NUMERIC dv type",
                producer.isRepeated(declaredMultiValued)
            );
        }
    }

    /**
     * End to end: a physically scalar file whose field is declared multi-valued must read its values
     * back through the multi-valued {@code SortedNumeric} API instead of erroring. Before the
     * physical gate, {@code getSortedNumeric} routed on the declaration alone and handed this scalar
     * column to the list reader, which fails the native list downcast on read. With the gate the
     * scalar reader serves it, singleton-wrapped, so each doc yields exactly its one value.
     * Falsifiable: reverting the gate to {@code if (isMultiValued(field))} makes this throw.
     */
    public void testScalarFileDeclaredMultiValuedReadsThroughMultiValuedApi() throws Exception {
        long[] values = { 11, 22, 33, 44 };
        Path file = createTempDir().resolve("scalar-read.parquet");
        writeLongScalarColumn(file, values);

        FieldInfo declaredMultiValued = fieldInfo(COLUMN, 0, DocValuesType.SORTED_NUMERIC);
        try (
            FSDirectory directory = FSDirectory.open(file.getParent());
            ParquetDocValuesProducer producer = new ParquetDocValuesProducer(
                segmentReadState(directory, file, values.length, declaredMultiValued),
                null
            );
            CursorRegistry cursors = new CursorRegistry()
        ) {
            SortedNumericDocValues dv = producer.getSortedNumeric(declaredMultiValued, cursors);
            for (int doc = 0; doc < values.length; doc++) {
                assertTrue("doc " + doc + " must be present", dv.advanceExact(doc));
                assertEquals("physically scalar doc " + doc + " must yield exactly one value", 1, dv.docValueCount());
                assertEquals("value at doc " + doc, values[doc], dv.nextValue());
            }
        }
    }

    private void writeLongScalarColumn(Path file, long[] values) throws Exception {
        Schema schema = new Schema(List.of(new Field(COLUMN, FieldType.nullable(new ArrowType.Int(64, true)), null)));

        NativeParquetWriter writer = new NativeParquetWriter(file.toString());
        ArrowSchema schemaExport = ArrowSchema.allocateNew(allocator);
        Data.exportSchema(allocator, schema, null, schemaExport);
        try (ArrowExport export = new ArrowExport(null, schemaExport)) {
            writer.initialize("test-index", export.getSchemaAddress(), ParquetSortConfig.empty(), 0L, false);
        }
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            BigIntVector vector = (BigIntVector) root.getVector(COLUMN);
            vector.allocateNew(values.length);
            for (int i = 0; i < values.length; i++) {
                vector.setSafe(i, values[i]);
            }
            vector.setValueCount(values.length);
            root.setRowCount(values.length);

            ArrowArray array = ArrowArray.allocateNew(allocator);
            ArrowSchema dataSchema = ArrowSchema.allocateNew(allocator);
            Data.exportVectorSchemaRoot(allocator, root, null, array, dataSchema);
            try (ArrowExport export = new ArrowExport(array, dataSchema)) {
                writer.write(export.getArrayAddress(), export.getSchemaAddress());
            }
        }
        writer.flush();
    }

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
