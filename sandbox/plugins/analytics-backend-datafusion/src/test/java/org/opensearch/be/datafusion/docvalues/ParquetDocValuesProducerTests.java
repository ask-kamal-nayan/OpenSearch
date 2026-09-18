/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.opensearch.be.datafusion.docvalues.bridge.DecodedListBatch;
import org.opensearch.be.datafusion.docvalues.bridge.ListValueReader;
import org.opensearch.be.datafusion.docvalues.iter.ParquetSortedNumericDocValues;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.parquet.bridge.ParquetFileMetadata;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;

public class ParquetDocValuesProducerTests extends OpenSearchTestCase {

    /**
     * The producer gates once per file on the stamped format version, admitting the inclusive
     * {@code [MIN, MAX]} window and rejecting everything outside it - too old, too new, and unstamped.
     * Driven with synthetic version longs so each boundary
     * is covered without writing a file per case.
     */
    public void testFormatVersionGateAcceptsRangeAndRejectsOutside() throws Exception {
        Path file = createTempDir().resolve("gate.parquet");

        // In range: the bounds themselves must pass.
        ParquetDocValuesProducer.checkFormatVersion(ParquetDocValuesProducer.MIN_SUPPORTED_FORMAT_VERSION, file);
        ParquetDocValuesProducer.checkFormatVersion(ParquetDocValuesProducer.MAX_SUPPORTED_FORMAT_VERSION, file);

        // Too old: one tick below the floor.
        expectThrows(
            IOException.class,
            () -> ParquetDocValuesProducer.checkFormatVersion(ParquetDocValuesProducer.MIN_SUPPORTED_FORMAT_VERSION - 1, file)
        );

        // Unstamped: the unknown sentinel is reported as carrying no version, not as a numeric one.
        IOException unstamped = expectThrows(
            IOException.class,
            () -> ParquetDocValuesProducer.checkFormatVersion(ParquetFileMetadata.FORMAT_VERSION_UNKNOWN, file)
        );
        assertTrue(
            "unstamped file must be reported as carrying no version",
            unstamped.getMessage().contains("no parseable opensearch.format_version")
        );

        // Too new: one tick above the ceiling must be refused rather than read with current-version logic.
        expectThrows(
            IOException.class,
            () -> ParquetDocValuesProducer.checkFormatVersion(ParquetDocValuesProducer.MAX_SUPPORTED_FORMAT_VERSION + 1, file)
        );
    }

    /**
     * The range's top must track the writer's current version, so a writer bump forces a deliberate
     * codec bump rather than silently reading a newer file with today's decode logic.
     */
    public void testSupportedRangeTracksTheWriterVersion() {
        assertTrue(
            "min must not exceed max",
            ParquetDocValuesProducer.MIN_SUPPORTED_FORMAT_VERSION <= ParquetDocValuesProducer.MAX_SUPPORTED_FORMAT_VERSION
        );
        assertEquals(
            "the range's top must equal the writer's current version",
            ParquetDataFormatPlugin.PARQUET_FORMAT_VERSION,
            ParquetDocValuesProducer.MAX_SUPPORTED_FORMAT_VERSION
        );
    }

    /**
     * The producer's multi-valued decision (W2): a field carrying SORTED_NUMERIC - the DV type W1
     * stamps only on genuinely multi-valued fields - is served by the list iterator, while a
     * single-valued NUMERIC field takes the singleton path. Driven through the same
     * {@code isMultiValued} predicate {@code getSortedNumeric} branches on, so a revert of W2 to
     * always-singleton flips the SORTED_NUMERIC case here.
     */
    public void testMultiValuedDecisionSelectsTheListPath() {
        assertTrue("SORTED_NUMERIC must be served as multi-valued", ParquetDocValuesProducer.isMultiValued(dvField(DocValuesType.SORTED_NUMERIC)));
        assertFalse("single-valued NUMERIC must not take the list path", ParquetDocValuesProducer.isMultiValued(dvField(DocValuesType.NUMERIC)));
    }

    /**
     * A genuine multi-valued iterator must NOT be a {@code DocValues.singleton} wrap: unwrapSingleton
     * returns null for it but non-null for the single-valued path. This pins the observable
     * difference W2 relies on - a value source unwrapping the result would collapse the list path
     * back to one value per doc if the two were confused.
     */
    public void testMultiValuedIteratorIsNotASingletonWrap() {
        SortedNumericDocValues multiValued = new ParquetSortedNumericDocValues(nullListReader(), 1, false);
        assertNull("the multi-valued iterator must not unwrap to a singleton", DocValues.unwrapSingleton(multiValued));

        SortedNumericDocValues singleValued = DocValues.singleton(constantNumeric());
        assertNotNull("the single-valued path must unwrap to its inner numeric iterator", DocValues.unwrapSingleton(singleValued));
    }

    /** Builds a synthetic doc-values {@link FieldInfo} carrying only the DV type under test. */
    private static FieldInfo dvField(DocValuesType dvType) {
        return new FieldInfo(
            "value",
            0,
            false,
            true,
            false,
            IndexOptions.NONE,
            dvType,
            DocValuesSkipIndexType.NONE,
            -1,
            new HashMap<>(),
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

    private static ListValueReader nullListReader() {
        return new ListValueReader() {
            @Override
            public DecodedListBatch decodedListBatch() {
                return null;
            }

            @Override
            public void loadListBatchContaining(long row) {
                // unwrapSingleton inspects only the iterator's type, so no batch is needed here.
            }
        };
    }

    private static NumericDocValues constantNumeric() {
        return new NumericDocValues() {
            @Override
            public long longValue() {
                return 0L;
            }

            @Override
            public boolean advanceExact(int target) {
                return true;
            }

            @Override
            public int docID() {
                return -1;
            }

            @Override
            public int nextDoc() {
                return NO_MORE_DOCS;
            }

            @Override
            public int advance(int target) {
                return NO_MORE_DOCS;
            }

            @Override
            public long cost() {
                return 0L;
            }
        };
    }
}
