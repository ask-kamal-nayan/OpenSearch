/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.recovery;

import org.apache.lucene.index.CorruptIndexException;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.recovery.FormatRecoveryCoordinator;
import org.opensearch.index.engine.exec.recovery.RecoveryContext;
import org.opensearch.index.engine.exec.recovery.RecoveryValidationException;
import org.opensearch.index.engine.exec.recovery.UploadContext;
import org.opensearch.parquet.engine.ParquetDataFormat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;

/**
 * Parquet implementation of {@link FormatRecoveryCoordinator}. Captures a small state blob at
 * upload (version + file count) and on recovery validates:
 * <ol>
 *   <li>state version matches this coordinator's version</li>
 *   <li>local parquet file count equals what was uploaded</li>
 *   <li>each parquet file on disk ends with the Parquet footer magic {@code PAR1}</li>
 * </ol>
 * Fail-fast on any mismatch so cluster allocation retries on another node.
 *
 * <p>Stateless singleton; registered by {@code ParquetDataFormatPlugin.getRecoveryCoordinator()}.
 */
@ExperimentalApi
public final class ParquetFormatRecoveryCoordinator implements FormatRecoveryCoordinator {

    /** State version — bump when the on-wire state layout changes. */
    private static final byte STATE_VERSION = 1;

    /** Parquet footer marker, per the Apache Parquet spec. Both first and last 4 bytes of every parquet file. */
    private static final byte[] PARQUET_MAGIC = { 'P', 'A', 'R', '1' };

    private static final int STATE_BYTES = 1 /* version */ + 4 /* file count */;

    @Override
    public String formatName() {
        return ParquetDataFormat.PARQUET_DATA_FORMAT_NAME;
    }

    @Override
    public byte[] captureForUpload(UploadContext context) throws IOException {
        int fileCount = countParquetFiles(context.snapshot().getSearchableFiles(formatName()));
        return ByteBuffer.allocate(STATE_BYTES).put(STATE_VERSION).putInt(fileCount).array();
    }

    @Override
    public void onRecovery(byte[] state, RecoveryContext context) throws IOException {
        if (state == null || state.length < STATE_BYTES) {
            context.ops()
                .failRecovery(formatName(), "parquet coordinator state too short (" + (state == null ? 0 : state.length) + " bytes)", null);
        }
        ByteBuffer buf = ByteBuffer.wrap(state);
        byte version = buf.get();
        if (version != STATE_VERSION) {
            context.ops()
                .failRecovery(
                    formatName(),
                    "parquet coordinator state version " + version + " unsupported (this coordinator understands v" + STATE_VERSION + ")",
                    null
                );
        }
        int expectedCount = buf.getInt();

        int localCount = countParquetFiles(context.snapshot().getSearchableFiles(formatName()));
        if (localCount != expectedCount) {
            context.ops()
                .failRecovery(formatName(), "parquet file count mismatch: expected=" + expectedCount + " local=" + localCount, null);
        }

        for (String fileName : context.ops().listFormatFiles(formatName())) {
            Path file = context.shardDataPath().resolve(formatName()).resolve(fileName);
            verifyParquetMagic(file);
        }
    }

    private static int countParquetFiles(Collection<WriterFileSet> fileSets) {
        if (fileSets == null) return 0;
        int total = 0;
        for (WriterFileSet wfs : fileSets) {
            total += wfs.files().size();
        }
        return total;
    }

    /** Verifies the last 4 bytes of {@code file} are the Parquet magic. Throws on mismatch. */
    private static void verifyParquetMagic(Path file) throws IOException {
        try (FileChannel fc = FileChannel.open(file, StandardOpenOption.READ)) {
            long size = fc.size();
            if (size < PARQUET_MAGIC.length) {
                throw new CorruptIndexException("parquet file too small (" + size + " bytes)", file.toString());
            }
            ByteBuffer magic = ByteBuffer.allocate(PARQUET_MAGIC.length);
            fc.read(magic, size - PARQUET_MAGIC.length);
            for (int i = 0; i < PARQUET_MAGIC.length; i++) {
                if (magic.get(i) != PARQUET_MAGIC[i]) {
                    throw new CorruptIndexException("parquet footer magic mismatch", file.toString());
                }
            }
        } catch (CorruptIndexException e) {
            throw new RecoveryValidationException(ParquetDataFormat.PARQUET_DATA_FORMAT_NAME, e.getMessage(), e);
        }
    }
}
