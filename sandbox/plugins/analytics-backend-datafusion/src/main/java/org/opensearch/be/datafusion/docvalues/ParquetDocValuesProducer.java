/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.opensearch.be.datafusion.docvalues.bridge.ParquetCodecBridge;
import org.opensearch.be.datafusion.docvalues.bridge.ParquetColumnReader;
import org.opensearch.be.datafusion.docvalues.bridge.ParquetListColumnReader;
import org.opensearch.be.datafusion.docvalues.iter.ParquetNumericDocValues;
import org.opensearch.be.datafusion.docvalues.iter.ParquetSortedNumericDocValues;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Read-only {@link DocValuesProducer} that serves single-valued numeric doc values from a Parquet
 * file through Lucene's DocValues iterator API.
 *
 * <p>The constructor resolves the backing file and sanity-checks its row count against the segment's
 * {@code maxDoc}, gates once on the stamped format version, but opens no cursor. It also captures the
 * store those bytes come from: a hot shard's Parquet files are on local disk, while a shard tiered to
 * warm keeps them only in the remote object store, reachable through the native store the engine
 * stamped on the segment.
 *
 * <p>One producer is cached per segment core by {@link ParquetDocValuesProducerRegistry} and shared
 * across requests; it is closed by the core's closed-listener, not per request. Each
 * {@link #getSortedNumeric(FieldInfo, CursorRegistry)} opens its own dedicated
 * {@link ParquetColumnReader}: a native cursor is forward-only, so one shared across concurrent
 * segment-search slices would be driven backwards by one slice while another advances it. A reader
 * per iterator keeps each slice's scan independent, and the cursor's lifetime belongs to the calling
 * request's {@link CursorRegistry} - the accessor API carries no request identity, so the producer
 * cannot own it.
 */
public final class ParquetDocValuesProducer extends DocValuesProducer {

    /** Oldest stamped format version this codec can decode, long-encoded as {@code major*1_000_000 + minor*1_000 + patch}. */
    static final long MIN_SUPPORTED_FORMAT_VERSION = 1_000_000L; // 1.0.0

    /**
     * Newest stamped format version this codec can decode. Deliberately a literal rather than a
     * reference to {@code ParquetDataFormatPlugin.PARQUET_FORMAT_VERSION}: tracking the writer
     * automatically would let a writer bump silently admit a file this decode logic has never seen.
     * {@code ParquetDocValuesProducerTests} asserts the two are equal, so a writer bump fails the build
     * until someone confirms the new version is readable and bumps this too.
     */
    static final long MAX_SUPPORTED_FORMAT_VERSION = 1_000_000L; // 1.0.0

    private final Path parquetFile;
    /**
     * Native object store every cursor reads {@link #parquetFile} through, or
     * {@link ParquetColumnReader#LOCAL_STORE} when the file is on local disk. Captured once from the
     * segment's stamp so a cursor opened later in the segment's life cannot disagree with the row count
     * validated here.
     */
    private final long storePointer;
    private final MapperService mapperService;
    /**
     * Index settings the decode-window sizes are resolved from, captured once so every cursor this
     * producer opens agrees. {@link Settings#EMPTY} when there is no mapper service, which only
     * happens in low-level tests.
     */
    private final Settings indexSettings;
    private final int maxDoc;
    private final long parquetRowCount;
    /**
     * Whether the writer proved each row's values already ascending, memoized once per segment from
     * the {@code opensearch.values_sorted} footer marker. Only a present-and-true marker sets this;
     * an absent or false marker leaves it false so the multi-valued iterator sorts on read (the
     * default-safe rule). Never re-read per document.
     */
    private final boolean valuesSorted;

    // The producer no longer owns a dedicatedReaders list: cursor lifecycle moved to the
    // request-scoped CursorRegistry (base #23055 refactor), which now also releases list cursors.
    private volatile boolean closed;

    /**
     * Memoized per-column physical shape (repeated vs scalar) read from the file's own schema. A
     * segment's shape is fixed once written, so the native probe runs at most once per column. Keyed
     * by field name, like the file itself. See {@link #isRepeated(FieldInfo)}.
     */
    private final Map<String, Boolean> repeatedColumns = new ConcurrentHashMap<>();

    /**
     * @param mapperService resolves OpenSearch mapping types for DV-type validation (may be
     *                      {@code null} only in low-level tests that bypass type validation)
     * @throws IOException if the backing Parquet file for the segment cannot be resolved
     * @throws IllegalStateException if the Parquet row count does not match the segment's {@code maxDoc}
     */
    public ParquetDocValuesProducer(SegmentReadState state, MapperService mapperService) throws IOException {
        this.mapperService = mapperService;
        this.indexSettings = mapperService == null ? Settings.EMPTY : mapperService.getIndexSettings().getSettings();
        this.maxDoc = state.segmentInfo.maxDoc();

        ParquetSegmentLayout.ParquetSource resolved = ParquetSegmentLayout.resolve(state);
        if (resolved == null) {
            throw new IOException(
                String.format(
                    Locale.ROOT,
                    "no Parquet file bound to segment '%s' (maxDoc=%d); cannot serve Parquet doc values",
                    state.segmentInfo.name,
                    maxDoc
                )
            );
        }
        this.parquetFile = resolved.file();
        this.storePointer = resolved.storePointer();

        ParquetCodecBridge.FileMetadata metadata = ParquetCodecBridge.fileMetadata(parquetFile.toString(), storePointer);
        checkFormatVersion(metadata.opensearchFormatVersion(), parquetFile);
        this.parquetRowCount = metadata.numRows();
        // Read the values-sorted marker once here, at segment open, and memoize it: the multi-valued
        // iterator needs it only to decide whether it may skip its read-side sort, never per document.
        // Only a present-and-true marker grants that permission; absent/false keeps the reader sorting.
        this.valuesSorted = metadata.valuesSorted();
        if (parquetRowCount != maxDoc) {
            throw new IllegalStateException(
                String.format(
                    Locale.ROOT,
                    "Parquet/Lucene row-count mismatch for segment '%s': Lucene maxDoc=%d but Parquet numRows=%d (file=%s)",
                    state.segmentInfo.name,
                    maxDoc,
                    parquetRowCount,
                    parquetFile
                )
            );
        }
    }

    /**
     * Test seam: builds over an already-resolved file, skipping segment resolution, the row-count
     * check, and the format-version gate. Never reached in production, where the segment-scoped
     * registry constructs the producer from a {@link SegmentReadState}.
     */
    ParquetDocValuesProducer(Path parquetFile, long storePointer, Settings indexSettings, int maxDoc, MapperService mapperService) {
        this.parquetFile = parquetFile;
        this.storePointer = storePointer;
        this.indexSettings = indexSettings;
        this.maxDoc = maxDoc;
        this.mapperService = mapperService;
        this.parquetRowCount = maxDoc;
        // Default-safe for the test seam: no footer metadata is read here, so the read-side iterator
        // sorts each document's values (an absent/false marker never grants the skip-sort permission).
        this.valuesSorted = false;
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        // Every field this codec serves is SORTED_NUMERIC (FieldTypeMapping), so per the
        // DocValuesProducer contract this accessor is never invoked for a valid FieldInfo.
        throw unsupported("numeric", field);
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) {
        // The doc-values accessor API carries no request identity; a cursor opened here would have no
        // request-scoped owner to close it. Callers go through the leaf wrapper, which supplies the
        // request's CursorRegistry via the overload below.
        throw new UnsupportedOperationException(
            "ParquetDocValuesProducer requires a request-scoped cursor registry; call getSortedNumeric(field, cursors)"
        );
    }

    /**
     * Serves {@code field} over a dedicated forward-only cursor, recorded on {@code cursors} so the
     * calling request closes it when it ends.
     *
     * <p>A physically repeated (Parquet LIST) column is served through the multi-valued
     * {@link ParquetSortedNumericDocValues} path; a single-valued column is wrapped as a singleton
     * via {@code DocValues.singleton}, which OpenSearch value sources recover with
     * {@code DocValues.unwrapSingleton}.
     */
    SortedNumericDocValues getSortedNumeric(FieldInfo field, CursorRegistry cursors) throws IOException {
        validate(field, DocValuesType.SORTED_NUMERIC);
        if (isMultiValued(field) && isRepeated(field)) {
            // Two signals, both required. The declaration (W1 stamped SORTED_NUMERIC) says the
            // mapping considers the field multi-valued; the physical probe says this segment's
            // column is actually a LIST on disk. They can legitimately disagree after a
            // scalar-to-LIST promotion: the mapping then reports multi-valued for every segment,
            // while segments written earlier are still physically scalar. Routing such an older
            // scalar segment to the list reader would fail the native list downcast, so it must fall
            // through to the singleton path below instead - the field stays consistently declared,
            // so queries and aggregations bind the same way across segments of differing shape.
            // Genuinely multi-valued (W1 stamped SORTED_NUMERIC on the synthetic FieldInfo): serve the
            // per-row value list. The iterator sorts each row ascending unless valuesSorted proves the
            // writer already did, so min/max are correct regardless of the marker.
            return new ParquetSortedNumericDocValues(openListCursor(field.getName(), cursors), maxDoc, valuesSorted);
        }
        // Single-valued numeric: every such column on disk holds one value per row, so this singleton
        // wrap is exact; OpenSearch value sources recover the inner iterator via DocValues.unwrapSingleton.
        // Also the disagreement case above (declared multi-valued, physically scalar): the scalar
        // reader serves the older segment correctly through the same SortedNumeric API.
        return DocValues.singleton(new ParquetNumericDocValues(openCursor(field.getName(), cursors), maxDoc));
    }

    /**
     * Cheap pre-check that the field's DV type is {@code SORTED_NUMERIC}. Under base #23055's uniform
     * mapping {@code ParquetDocValuesLeafReader} (W1) stamps SORTED_NUMERIC on every field this codec
     * serves, so this is effectively always true; the authoritative multi-valued signal is the
     * on-disk physical-shape probe in {@link #isRepeated(FieldInfo)}, which {@code getSortedNumeric}
     * ANDs with this guard. Kept because it cannot change behaviour and documents the routing contract.
     */
    static boolean isMultiValued(FieldInfo field) {
        return field.getDocValuesType() == DocValuesType.SORTED_NUMERIC;
    }

    /**
     * Whether {@code field}'s column is physically repeated (a Parquet LIST) in this segment, read
     * from the file's own schema rather than the mapping.
     *
     * <p>The mapping and the file legitimately disagree exactly where it matters: once a numeric is
     * promoted from scalar to LIST ({@code MultiValueState.AUTO -> LIST}) the mapping declares it
     * multi-valued for every segment, while segments written before the promotion are still scalar
     * on disk. Routing on the declaration alone would hand those older scalar columns to the list
     * reader and fail the native list downcast, so {@link #getSortedNumeric} gates the list path on
     * this on-disk signal as well.
     *
     * <p>Opens a short-lived probe cursor whose only job is to read the shape recorded at open - it
     * never advances the cursor - and memoizes the answer in {@link #repeatedColumns}, since a
     * segment's physical shape is fixed once written.
     */
    boolean isRepeated(FieldInfo field) throws IOException {
        Boolean cached = repeatedColumns.get(field.getName());
        if (cached != null) {
            return cached;
        }
        boolean repeated;
        try (ParquetColumnReader probe = ParquetColumnReader.open(parquetFile, field.getName(), indexSettings, storePointer)) {
            repeated = probe.isPhysicallyRepeated();
        }
        repeatedColumns.put(field.getName(), repeated);
        return repeated;
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) {
        throw unsupported("binary", field);
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) {
        throw unsupported("sorted", field);
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) {
        throw unsupported("sorted-set", field);
    }

    /** No DocValues skip index is served; the synthetic {@code FieldInfo}s advertise skip type NONE. */
    @Override
    public DocValuesSkipper getSkipper(FieldInfo field) {
        return null;
    }

    /**
     * Verifies the backing Parquet file is still accessible and its row count matches the value
     * cached at construction.
     *
     * <p>Not currently invoked: this producer is a search-time overlay, not a registered
     * {@code DocValuesFormat}, so codec-driven integrity checks (CheckIndex, merge-time verification)
     * do not reach it.
     */
    @Override
    public void checkIntegrity() throws IOException {
        ParquetCodecBridge.FileMetadata metadata = ParquetCodecBridge.fileMetadata(parquetFile.toString(), storePointer);
        if (metadata.numRows() != parquetRowCount) {
            throw new IOException(
                String.format(
                    Locale.ROOT,
                    "checkIntegrity: Parquet numRows changed for %s: expected %d, found %d",
                    parquetFile,
                    parquetRowCount,
                    metadata.numRows()
                )
            );
        }
    }

    @Override
    public void close() {
        // Segment-lifetime producer, closed once by the core's closed-listener. It holds no open file
        // handle in this codec - each cursor opens its own - so close only marks the producer done;
        // request cursors are closed by the request's CursorRegistry, not here.
        closed = true;
    }

    /**
     * Rejects a file this codec cannot decode: unstamped, older than {@link #MIN_SUPPORTED_FORMAT_VERSION},
     * or newer than {@link #MAX_SUPPORTED_FORMAT_VERSION}, failing on an out-of-range file rather than reading it
     * with assumptions that may not hold.
     */
    static void checkFormatVersion(long formatVersion, Path file) throws IOException {
        if (formatVersion == ParquetCodecBridge.FORMAT_VERSION_UNKNOWN) {
            throw new IOException(
                String.format(
                    Locale.ROOT,
                    "Parquet file %s carries no parseable opensearch.format_version; this doc-values codec requires a stamped version in %s",
                    file,
                    supportedRange()
                )
            );
        }
        if (formatVersion < MIN_SUPPORTED_FORMAT_VERSION || formatVersion > MAX_SUPPORTED_FORMAT_VERSION) {
            throw new IOException(
                String.format(
                    Locale.ROOT,
                    "Parquet file %s has OpenSearch format version %s, outside this doc-values codec's supported range %s",
                    file,
                    describeFormatVersion(formatVersion),
                    supportedRange()
                )
            );
        }
    }

    /** Scales of the long-encoded {@code major.minor.patch} version: {@code major*1_000_000 + minor*1_000 + patch}. */
    private static final long MAJOR_SCALE = 1_000_000L;
    private static final long MINOR_SCALE = 1_000L;

    /** Renders the inclusive supported version range for an error message. */
    private static String supportedRange() {
        return "[" + describeFormatVersion(MIN_SUPPORTED_FORMAT_VERSION) + ", " + describeFormatVersion(MAX_SUPPORTED_FORMAT_VERSION) + "]";
    }

    /** Renders a long-encoded format version as {@code major.minor.patch} for an error message. */
    private static String describeFormatVersion(long formatVersion) {
        long major = formatVersion / MAJOR_SCALE;
        long minor = formatVersion / MINOR_SCALE % MINOR_SCALE;
        long patch = formatVersion % MINOR_SCALE;
        return major + "." + minor + "." + patch;
    }

    /** Validates the field's mapping type supports the requested DV type, when a mapper is present. */
    private void validate(FieldInfo field, DocValuesType requested) {
        if (mapperService == null) {
            return; // low-level tests may bypass mapping validation
        }
        FieldTypeMapping.validate(field.getName(), mappingType(field), requested);
    }

    private String mappingType(FieldInfo field) {
        MappedFieldType mft = mapperService.fieldType(field.getName());
        if (mft == null) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "field '%s' has no mapping; cannot resolve Parquet column type", field.getName())
            );
        }
        return mft.typeName();
    }

    /**
     * Opens a dedicated forward-only cursor for one iterator and records it on the request's
     * {@code cursors}, which closes it at request end.
     */
    private ParquetColumnReader openCursor(String field, CursorRegistry cursors) throws IOException {
        ParquetColumnReader reader = ParquetColumnReader.open(parquetFile, field, indexSettings, storePointer);
        cursors.register(reader);
        return reader;
    }

    /**
     * Opens a dedicated forward-only list cursor for one multi-valued iterator and records it on the
     * request's {@code cursors}. List cursors are released on the same request lifecycle as scalar
     * cursors - the registry was widened to accept both - so no native handle leaks.
     */
    private ParquetListColumnReader openListCursor(String field, CursorRegistry cursors) throws IOException {
        ParquetListColumnReader reader = ParquetListColumnReader.open(parquetFile, field, indexSettings, storePointer);
        cursors.register(reader);
        return reader;
    }

    private UnsupportedOperationException unsupported(String kind, FieldInfo field) {
        return new UnsupportedOperationException(
            String.format(
                Locale.ROOT,
                "Parquet DocValues codec does not serve %s doc values (field '%s'); sorted-numeric only",
                kind,
                field.getName()
            )
        );
    }

    /** Whether {@link #close()} has run. */
    boolean isClosed() {
        return closed;
    }
}
