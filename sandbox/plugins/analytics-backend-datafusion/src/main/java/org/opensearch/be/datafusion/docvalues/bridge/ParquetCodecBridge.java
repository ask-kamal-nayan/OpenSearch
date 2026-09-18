/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues.bridge;

import org.opensearch.nativebridge.spi.NativeCall;
import org.opensearch.nativebridge.spi.NativeLibraryLoader;

import java.io.IOException;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * FFM bridge for the Parquet read codec: binds the forward-only column-cursor downcalls exported by
 * the native library. Kept separate from the write-path {@code RustBridge} so the read and write
 * native surfaces stay independent. The cursor is column-oriented rather than doc-values specific,
 * so later codec parts (binary/keyword columns, a doc-values skipper) bind their downcalls here too.
 */
public final class ParquetCodecBridge {

    private static final MethodHandle OPEN_CURSOR;
    private static final MethodHandle CLOSE_CURSOR;
    private static final MethodHandle RESET_CURSOR;
    private static final MethodHandle NEXT_BATCH;
    private static final MethodHandle NEXT_LIST_BATCH;
    private static final MethodHandle FILE_METADATA;
    private static final MethodHandle IS_REPEATED;

    /** Status returned by {@link #nextBatch} when a batch was produced. */
    /**
     * Value of {@link FileMetadata#opensearchFormatVersion} when the footer carries no parseable
     * stamp. Mirrors {@code ParquetFileMetadata.FORMAT_VERSION_UNKNOWN} in the parquet-data-format
     * plugin, whose writer stamps the version this reader gates on.
     */
    public static final long FORMAT_VERSION_UNKNOWN = 0L;

    public static final long RC_OK = 0L;
    /** Status returned by {@link #nextBatch} when the cursor is exhausted. A {@code < 0} return is an error pointer. */
    public static final long RC_EOF = 2L;

    /**
     * Tri-state values of {@link FileMetadata#valuesSorted}, mirroring the {@code VALUES_SORTED_*}
     * wire constants in the native {@code doc_values_cursor}. The distinction between
     * {@link #VALUES_SORTED_FALSE} and {@link #VALUES_SORTED_ABSENT} is load-bearing and must not be
     * collapsed: absent is every file written before the ingest-sort feature (and every file written
     * with it off), so a reader must sort those, whereas only {@link #VALUES_SORTED_TRUE} is
     * permission to skip the read-side per-row sort.
     */
    public static final long VALUES_SORTED_TRUE = 1L;
    public static final long VALUES_SORTED_FALSE = 0L;
    public static final long VALUES_SORTED_ABSENT = -1L;

    static {
        SymbolLookup lib = NativeLibraryLoader.symbolLookup();
        Linker linker = Linker.nativeLinker();
        OPEN_CURSOR = linker.downcallHandle(
            lib.find("parquet_df_open_iter").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,    // file_ptr
                ValueLayout.JAVA_LONG,  // file_len
                ValueLayout.ADDRESS,    // column_ptr
                ValueLayout.JAVA_LONG,  // column_len
                ValueLayout.JAVA_LONG,  // initial_batch_size
                ValueLayout.JAVA_LONG,  // max_batch_size
                ValueLayout.JAVA_LONG   // store_ptr
            )
        );
        CLOSE_CURSOR = linker.downcallHandle(
            lib.find("parquet_df_close_iter").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );
        RESET_CURSOR = linker.downcallHandle(
            lib.find("parquet_df_reset_iter").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );
        NEXT_BATCH = linker.downcallHandle(
            lib.find("parquet_df_next_batch").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,  // handle
                ValueLayout.JAVA_LONG,  // target_row
                ValueLayout.ADDRESS,    // out_first_row
                ValueLayout.ADDRESS,    // out_last_row
                ValueLayout.ADDRESS,    // out_values_addr
                ValueLayout.ADDRESS,    // out_validity_addr
                ValueLayout.ADDRESS,    // out_validity_bit_offset
                ValueLayout.ADDRESS,    // out_value_kind
                ValueLayout.ADDRESS     // out_value_bit_offset
            )
        );
        NEXT_LIST_BATCH = linker.downcallHandle(
            lib.find("parquet_df_next_list_batch").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,  // handle
                ValueLayout.JAVA_LONG,  // target_row
                ValueLayout.ADDRESS,    // out_first_row
                ValueLayout.ADDRESS,    // out_last_row
                ValueLayout.ADDRESS,    // out_values_addr
                ValueLayout.ADDRESS,    // out_validity_addr
                ValueLayout.ADDRESS,    // out_validity_bit_offset
                ValueLayout.ADDRESS,    // out_value_kind
                ValueLayout.ADDRESS,    // out_value_bit_offset
                ValueLayout.ADDRESS,    // out_offsets_addr
                ValueLayout.ADDRESS     // out_value_count
            )
        );
        FILE_METADATA = linker.downcallHandle(
            lib.find("parquet_df_file_metadata").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,    // file_ptr
                ValueLayout.JAVA_LONG,  // file_len
                ValueLayout.JAVA_LONG,  // store_ptr
                ValueLayout.ADDRESS,    // out_num_rows
                ValueLayout.ADDRESS,    // out_format_version
                ValueLayout.ADDRESS     // out_values_sorted
            )
        );
        // Matches the Rust `parquet_df_is_repeated(handle: i64) -> i64` exactly: one JAVA_LONG
        // argument (the cursor handle), JAVA_LONG return. A FunctionDescriptor mismatch here fails
        // at call time, not compile time, so the arity and order are verified against the extern.
        IS_REPEATED = linker.downcallHandle(
            lib.find("parquet_df_is_repeated").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG) // handle
        );
    }

    /**
     * A Parquet file's row count and stamped OpenSearch format version.
     *
     * @param numRows                 rows in the file, which must equal the Lucene segment's {@code maxDoc}
     * @param opensearchFormatVersion the {@code opensearch.format_version} footer stamp, long-encoded as
     *                                {@code major*1_000_000 + minor*1_000 + patch}, or
     *                                {@link #FORMAT_VERSION_UNKNOWN}
     *                                if the file carries no parseable stamp
     * @param valuesSorted            tri-state {@code opensearch.values_sorted} footer marker:
     *                                {@link #VALUES_SORTED_TRUE}, {@link #VALUES_SORTED_FALSE}, or
     *                                {@link #VALUES_SORTED_ABSENT}. Absent must stay distinct from
     *                                false: only true is permission to skip the read-side per-row sort
     */
    public record FileMetadata(long numRows, long opensearchFormatVersion, long valuesSorted) {
    }

    /**
     * Reads {@code file}'s row count and format-version stamp through the same store and footer cache a
     * cursor over that file would use.
     *
     * <p>Distinct from {@code RustBridge.getFileMetadata}, which opens the path as a local file: a warm
     * shard's Parquet files exist only in its object store, so they are reachable only through
     * {@code storePtr}.
     *
     * @param storePtr native object store to read through, or {@code 0} for a local file
     */
    public static FileMetadata fileMetadata(String file, long storePtr) throws IOException {
        try (var call = new NativeCall()) {
            var f = call.str(file);
            var numRowsOut = call.longOut();
            var formatVersionOut = call.longOut();
            var valuesSortedOut = call.longOut();
            call.invokeIO(FILE_METADATA, f.segment(), f.len(), storePtr, numRowsOut, formatVersionOut, valuesSortedOut);
            return new FileMetadata(
                numRowsOut.get(ValueLayout.JAVA_LONG, 0),
                formatVersionOut.get(ValueLayout.JAVA_LONG, 0),
                valuesSortedOut.get(ValueLayout.JAVA_LONG, 0)
            );
        }
    }

    /**
     * Opens a forward-only cursor over one Parquet column and returns its native handle.
     *
     * @param initialBatchSize rows in the first decode window; must be in {@code 1..=maxBatchSize}
     * @param maxBatchSize     ceiling the adaptive window grows to, for this cursor's lifetime
     * @param storePtr         native object-store pointer the cursor reads {@code file} through, or
     *                         {@code 0} to read from the local filesystem. A warm shard's Parquet
     *                         files live in the remote object store, so it passes the pointer from
     *                         {@code ParquetDataFormatStoreHandler.getFormatStoreHandle()}; a hot
     *                         shard's files are local and pass {@code 0}.
     */
    public static long openColumnCursor(String file, String column, long initialBatchSize, long maxBatchSize, long storePtr)
        throws IOException {
        try (var call = new NativeCall()) {
            var f = call.str(file);
            var c = call.str(column);
            return call.invokeIO(OPEN_CURSOR, f.segment(), f.len(), c.segment(), c.len(), initialBatchSize, maxBatchSize, storePtr);
        }
    }

    /** Releases a cursor handle. */
    public static void closeColumnCursor(long handle) throws IOException {
        try (var call = new NativeCall()) {
            call.invokeIO(CLOSE_CURSOR, handle);
        }
    }

    /** Rewinds a cursor to row zero, retaining cached file metadata. */
    public static void resetColumnCursor(long handle) throws IOException {
        try (var call = new NativeCall()) {
            call.invokeIO(RESET_CURSOR, handle);
        }
    }

    /**
     * Whether the cursor's projected column is physically repeated (a Parquet LIST) on disk, read
     * from the file's own schema rather than the mapping. The two legitimately disagree after a
     * scalar-to-LIST promotion: the mapping then reports LIST for every segment while segments
     * written earlier are still scalar, so a caller must route each segment on this on-disk signal.
     * Reads only the schema recorded at open, so it never advances the cursor. Returns {@code true}
     * for repeated and {@code false} for scalar; a {@code < 0} native return is decoded into an
     * {@link IOException}.
     */
    public static boolean isRepeated(long handle) throws IOException {
        try (var call = new NativeCall()) {
            return call.invokeIO(IS_REPEATED, handle) == 1L;
        }
    }

    /**
     * Advances the cursor to the batch containing {@code targetRow}, writing the batch row range,
     * the borrowed Arrow value and validity buffer addresses, the validity bit offset, the value
     * KIND, and the value bit offset into the caller-owned out-parameters. Returns {@link #RC_OK}
     * or {@link #RC_EOF}; a {@code < 0} return is decoded into an {@link IOException}.
     *
     * <p>{@code outValueBitOffset} is meaningful only for the bit-packed boolean KIND; the
     * byte-addressed kinds fold their offset into {@code outValuesAddr} and report zero.
     */
    public static long nextBatch(
        long handle,
        long targetRow,
        MemorySegment outFirstRow,
        MemorySegment outLastRow,
        MemorySegment outValuesAddr,
        MemorySegment outValidityAddr,
        MemorySegment outValidityBitOffset,
        MemorySegment outValueKind,
        MemorySegment outValueBitOffset
    ) throws IOException {
        try (var call = new NativeCall()) {
            return call.invokeIO(
                NEXT_BATCH,
                handle,
                targetRow,
                outFirstRow,
                outLastRow,
                outValuesAddr,
                outValidityAddr,
                outValidityBitOffset,
                outValueKind,
                outValueBitOffset
            );
        }
    }

    /**
     * List-aware sibling of {@link #nextBatch} for repeated (multi-valued) columns. Writes the same
     * batch row range and borrowed flat value/validity buffers as {@link #nextBatch}, plus two
     * out-parameters describing the per-row list structure: {@code outOffsetsAddr}, the borrowed
     * {@code i32} offsets buffer positioned at row 0, and {@code outValueCount}, the number of child
     * values backing the batch. Row {@code r}'s values are the child range
     * {@code offsets[r]..offsets[r + 1]}; a caller reads {@code lastRow - firstRow + 2} offsets.
     * Returns {@link #RC_OK} or {@link #RC_EOF}; a {@code < 0} return is decoded into an
     * {@link IOException}.
     *
     * <p>A distinct native symbol from {@link #nextBatch} on purpose: the single-valued
     * {@code parquet_df_next_batch} ABI is depended on by the shipped read path, so it stays stable
     * and the list structure is carried by this sibling instead.
     *
     * <p>Not yet wired into any doc-values iterator; that is a later commit.
     */
    public static long nextListBatch(
        long handle,
        long targetRow,
        MemorySegment outFirstRow,
        MemorySegment outLastRow,
        MemorySegment outValuesAddr,
        MemorySegment outValidityAddr,
        MemorySegment outValidityBitOffset,
        MemorySegment outValueKind,
        MemorySegment outValueBitOffset,
        MemorySegment outOffsetsAddr,
        MemorySegment outValueCount
    ) throws IOException {
        try (var call = new NativeCall()) {
            return call.invokeIO(
                NEXT_LIST_BATCH,
                handle,
                targetRow,
                outFirstRow,
                outLastRow,
                outValuesAddr,
                outValidityAddr,
                outValidityBitOffset,
                outValueKind,
                outValueBitOffset,
                outOffsetsAddr,
                outValueCount
            );
        }
    }

    private ParquetCodecBridge() {}
}
