/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
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
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.opensearch.nativebridge.spi.ArrowExport;
import org.opensearch.parquet.bridge.NativeParquetWriter;
import org.opensearch.parquet.bridge.ParquetSortConfig;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public class DataFusionDocValuesTests extends OpenSearchTestCase {

    private static final int ROW_COUNT = 4;

    private BufferAllocator allocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        RustBridge.initLogger();
        allocator = new RootAllocator();
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    public void testRepeatedTypesUseDataFusionWithoutCodecNativeFallback() throws Exception {
        Path directoryPath = createTempDir();
        Path parquetFile = directoryPath.resolve("repeated.parquet");
        writeRepeatedFile(parquetFile);

        FieldInfo numbers = fieldInfo("numbers", 0, DocValuesType.SORTED_NUMERIC);
        FieldInfo tags = fieldInfo("tags", 1, DocValuesType.SORTED_SET);
        long dataFusionReadersBefore = RustBridge.dfOpenIterCount();

        try (
            FSDirectory directory = FSDirectory.open(directoryPath);
            ParquetDocValuesProducer producer = new ParquetDocValuesProducer(segmentReadState(directory, parquetFile, numbers, tags), null)
        ) {
            SortedNumericDocValues numeric = producer.getSortedNumeric(numbers);
            assertEquals(dataFusionReadersBefore + 1, RustBridge.dfOpenIterCount());

            assertTrue(numeric.advanceExact(0));
            assertEquals(2, numeric.docValueCount());
            assertEquals(1L, numeric.nextValue());
            assertEquals(3L, numeric.nextValue());
            assertFalse(numeric.advanceExact(1));
            assertFalse(numeric.advanceExact(2));
            assertTrue(numeric.advanceExact(3));
            assertEquals(3, numeric.docValueCount());
            assertEquals(5L, numeric.nextValue());
            assertEquals(8L, numeric.nextValue());
            assertEquals(8L, numeric.nextValue());

            SortedSetDocValues sortedSet = producer.getSortedSet(tags);
            assertEquals(dataFusionReadersBefore + 2, RustBridge.dfOpenIterCount());

            assertTrue(sortedSet.advanceExact(0));
            assertEquals(2, sortedSet.docValueCount());
            assertEquals(new BytesRef("alpha"), sortedSet.lookupOrd(sortedSet.nextOrd()));
            assertEquals(new BytesRef("beta"), sortedSet.lookupOrd(sortedSet.nextOrd()));
            assertFalse(sortedSet.advanceExact(1));
            assertFalse(sortedSet.advanceExact(2));
            assertTrue(sortedSet.advanceExact(3));
            assertEquals(2, sortedSet.docValueCount());
            assertEquals(new BytesRef("alpha"), sortedSet.lookupOrd(sortedSet.nextOrd()));
            assertEquals(new BytesRef("omega"), sortedSet.lookupOrd(sortedSet.nextOrd()));

        }

        assertEquals(dataFusionReadersBefore, RustBridge.dfOpenIterCount());
    }

    /**
     * The routing decision in {@link ParquetDocValuesLeafReader#getSortedNumericDocValues} must come
     * from the column's physical shape in the file, not from its declared Lucene doc-values type or
     * the mapping. Both columns below are declared {@code SORTED_NUMERIC}; only one is physically a
     * LIST, so an implementation that consulted the declared type would report both as repeated.
     */
    public void testIsRepeatedReflectsPhysicalColumnShapeNotDeclaredType() throws Exception {
        Path directoryPath = createTempDir();
        Path repeatedFile = directoryPath.resolve("repeated-shape.parquet");
        Path scalarFile = directoryPath.resolve("scalar-shape.parquet");
        writeRepeatedFile(repeatedFile);
        writeScalarFile(scalarFile);

        FieldInfo numbers = fieldInfo("numbers", 0, DocValuesType.SORTED_NUMERIC);
        FieldInfo tags = fieldInfo("tags", 1, DocValuesType.SORTED_SET);
        FieldInfo plain = fieldInfo("plain", 0, DocValuesType.SORTED_NUMERIC);

        try (
            FSDirectory directory = FSDirectory.open(directoryPath);
            ParquetDocValuesProducer producer = new ParquetDocValuesProducer(segmentReadState(directory, repeatedFile, numbers, tags), null)
        ) {
            assertTrue("LIST numeric column must report repeated", producer.isRepeated(numbers));
            assertTrue("LIST binary column must report repeated", producer.isRepeated(tags));
            // Memoised: a second call must agree with the first.
            assertTrue(producer.isRepeated(numbers));
        }

        try (
            FSDirectory directory = FSDirectory.open(directoryPath);
            ParquetDocValuesProducer producer = new ParquetDocValuesProducer(segmentReadState(directory, scalarFile, plain), null)
        ) {
            assertFalse("scalar column must not report repeated despite a SORTED_NUMERIC dv type", producer.isRepeated(plain));
        }
    }

    /** A file whose only column is a plain (non-LIST) int64, for the scalar side of shape detection. */
    private void writeScalarFile(Path file) throws Exception {
        Schema schema = new Schema(List.of(new Field("plain", FieldType.nullable(new ArrowType.Int(64, true)), null)));

        NativeParquetWriter writer = new NativeParquetWriter(file.toString());
        try (ArrowExport schemaExport = exportSchema(schema)) {
            writer.initialize("test-index", schemaExport.getSchemaAddress(), ParquetSortConfig.empty(), 0L);
        }
        try (ArrowExport dataExport = exportScalarData(schema)) {
            writer.write(dataExport.getArrayAddress(), dataExport.getSchemaAddress());
        }
        writer.flush();
    }

    private ArrowExport exportScalarData(Schema schema) {
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            BigIntVector vector = (BigIntVector) root.getVector("plain");
            for (int row = 0; row < ROW_COUNT; row++) {
                vector.setSafe(row, 10L * (row + 1));
            }
            vector.setValueCount(ROW_COUNT);
            root.setRowCount(ROW_COUNT);

            ArrowArray array = ArrowArray.allocateNew(allocator);
            ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
            Data.exportVectorSchemaRoot(allocator, root, null, array, arrowSchema);
            return new ArrowExport(array, arrowSchema);
        }
    }

    private void writeRepeatedFile(Path file) throws Exception {
        Field numberItem = new Field("item", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field tagItem = new Field("item", FieldType.nullable(new ArrowType.Utf8()), null);
        Schema schema = new Schema(
            List.of(
                new Field("numbers", FieldType.nullable(ArrowType.List.INSTANCE), List.of(numberItem)),
                new Field("tags", FieldType.nullable(ArrowType.List.INSTANCE), List.of(tagItem))
            )
        );

        NativeParquetWriter writer = new NativeParquetWriter(file.toString());
        try (ArrowExport schemaExport = exportSchema(schema)) {
            writer.initialize("test-index", schemaExport.getSchemaAddress(), ParquetSortConfig.empty(), 0L);
        }
        try (ArrowExport dataExport = exportData(schema)) {
            writer.write(dataExport.getArrayAddress(), dataExport.getSchemaAddress());
        }
        writer.flush();
    }

    private ArrowExport exportSchema(Schema schema) {
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
        Data.exportSchema(allocator, schema, null, arrowSchema);
        return new ArrowExport(null, arrowSchema);
    }

    private ArrowExport exportData(Schema schema) {
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            writeNumbers((ListVector) root.getVector("numbers"));
            writeTags((ListVector) root.getVector("tags"));
            root.setRowCount(ROW_COUNT);

            ArrowArray array = ArrowArray.allocateNew(allocator);
            ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
            Data.exportVectorSchemaRoot(allocator, root, null, array, arrowSchema);
            return new ArrowExport(array, arrowSchema);
        }
    }

    private static void writeNumbers(ListVector vector) {
        // Written pre-sorted ascending: NativeParquetWriter stamps the values-sorted marker (the
        // ingest path sorts in ParquetField#writeList), so the reader trusts on-disk order and skips
        // its per-visit sort. A representative fixture therefore stores ascending values.
        UnionListWriter writer = vector.getWriter();
        writer.setPosition(0);
        writer.startList();
        writer.writeBigInt(1);
        writer.writeBigInt(3);
        writer.endList();
        writer.setPosition(1);
        writer.writeNull();
        writer.setPosition(2);
        writer.startList();
        writer.endList();
        writer.setPosition(3);
        writer.startList();
        writer.writeBigInt(5);
        writer.writeBigInt(8);
        writer.writeBigInt(8);
        writer.endList();
        writer.setValueCount(ROW_COUNT);
    }

    private static void writeTags(ListVector vector) {
        // Written pre-sorted (UTF-8 byte order) and, for doc 3, with the duplicate adjacent: the
        // file is marked values-sorted so the reader skips the sort but STILL de-duplicates, which
        // relies on equal values being adjacent (guaranteed by the ingest sort).
        UnionListWriter writer = vector.getWriter();
        writer.setPosition(0);
        writer.startList();
        writer.writeVarChar("alpha");
        writer.writeVarChar("beta");
        writer.endList();
        writer.setPosition(1);
        writer.writeNull();
        writer.setPosition(2);
        writer.startList();
        writer.endList();
        writer.setPosition(3);
        writer.startList();
        writer.writeVarChar("alpha");
        writer.writeVarChar("omega");
        writer.writeVarChar("omega");
        writer.endList();
        writer.setValueCount(ROW_COUNT);
    }

    private static SegmentReadState segmentReadState(FSDirectory directory, Path parquetFile, FieldInfo... fields) {
        SegmentInfo segmentInfo = new SegmentInfo(
            directory,
            Version.LATEST,
            Version.LATEST,
            "_0",
            ROW_COUNT,
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
