/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.recovery;

import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.recovery.RecoveryContext;
import org.opensearch.index.engine.exec.recovery.RecoveryOps;
import org.opensearch.index.engine.exec.recovery.RecoveryValidationException;
import org.opensearch.index.engine.exec.recovery.UploadContext;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class ParquetFormatRecoveryCoordinatorTests extends OpenSearchTestCase {

    private ParquetFormatRecoveryCoordinator coordinator;
    private Path shardDataPath;
    private Path parquetDir;

    @Before
    public void setup() throws IOException {
        coordinator = new ParquetFormatRecoveryCoordinator();
        shardDataPath = createTempDir();
        parquetDir = shardDataPath.resolve("parquet");
        Files.createDirectories(parquetDir);
    }

    // --- captureForUpload ---

    public void testCaptureForUploadReturnsVersionedCount() throws IOException {
        UploadContext ctx = uploadContextWithParquetFiles(3);
        byte[] state = coordinator.captureForUpload(ctx);
        assertEquals("state should be 5 bytes (version + int count)", 5, state.length);
        ByteBuffer buf = ByteBuffer.wrap(state);
        assertEquals((byte) 1, buf.get());   // STATE_VERSION
        assertEquals(3, buf.getInt());
    }

    public void testCaptureForUploadZeroFiles() throws IOException {
        UploadContext ctx = uploadContextWithParquetFiles(0);
        byte[] state = coordinator.captureForUpload(ctx);
        ByteBuffer buf = ByteBuffer.wrap(state);
        assertEquals((byte) 1, buf.get());
        assertEquals(0, buf.getInt());
    }

    // --- onRecovery success ---

    public void testOnRecoverySucceedsWhenCountAndMagicMatch() throws IOException {
        writeParquetFile("_parquet_file_gen_1.parquet", validParquetBody());
        writeParquetFile("_parquet_file_gen_2.parquet", validParquetBody());

        byte[] state = versionedState((byte) 1, 2);
        RecoveryContext ctx = recoveryContextWithParquetFiles(
            List.of("_parquet_file_gen_1.parquet", "_parquet_file_gen_2.parquet"),
            new String[] { "_parquet_file_gen_1.parquet", "_parquet_file_gen_2.parquet" }
        );
        coordinator.onRecovery(state, ctx);   // should not throw
    }

    public void testOnRecoverySucceedsWithNoParquetFiles() throws IOException {
        byte[] state = versionedState((byte) 1, 0);
        RecoveryContext ctx = recoveryContextWithParquetFiles(List.of(), new String[0]);
        coordinator.onRecovery(state, ctx);   // should not throw
    }

    // --- onRecovery failure paths ---

    public void testOnRecoveryFailsOnStateVersionMismatch() throws IOException {
        byte[] state = versionedState((byte) 99, 0);
        RecoveryContext ctx = recoveryContextWithParquetFiles(List.of(), new String[0]);
        RecoveryValidationException e = expectThrows(RecoveryValidationException.class, () -> coordinator.onRecovery(state, ctx));
        assertTrue(e.getMessage(), e.getMessage().contains("state version 99 unsupported"));
    }

    public void testOnRecoveryFailsOnCountMismatch() throws IOException {
        writeParquetFile("_parquet_file_gen_1.parquet", validParquetBody());

        byte[] state = versionedState((byte) 1, 2);   // expects 2, disk has 1
        RecoveryContext ctx = recoveryContextWithParquetFiles(
            List.of("_parquet_file_gen_1.parquet"),
            new String[] { "_parquet_file_gen_1.parquet" }
        );
        RecoveryValidationException e = expectThrows(RecoveryValidationException.class, () -> coordinator.onRecovery(state, ctx));
        assertTrue(e.getMessage(), e.getMessage().contains("file count mismatch"));
        assertTrue(e.getMessage(), e.getMessage().contains("expected=2"));
        assertTrue(e.getMessage(), e.getMessage().contains("local=1"));
    }

    public void testOnRecoveryFailsOnCorruptMagicBytes() throws IOException {
        // File ending in "XXXX" instead of "PAR1"
        byte[] body = new byte[64];
        body[60] = 'X';
        body[61] = 'X';
        body[62] = 'X';
        body[63] = 'X';
        writeParquetFile("_parquet_file_gen_1.parquet", body);

        byte[] state = versionedState((byte) 1, 1);
        RecoveryContext ctx = recoveryContextWithParquetFiles(
            List.of("_parquet_file_gen_1.parquet"),
            new String[] { "_parquet_file_gen_1.parquet" }
        );
        RecoveryValidationException e = expectThrows(RecoveryValidationException.class, () -> coordinator.onRecovery(state, ctx));
        assertTrue(e.getMessage(), e.getMessage().contains("parquet footer magic mismatch"));
    }

    public void testOnRecoveryFailsOnTruncatedFile() throws IOException {
        writeParquetFile("_parquet_file_gen_1.parquet", new byte[] { 'P', 'A' });   // only 2 bytes

        byte[] state = versionedState((byte) 1, 1);
        RecoveryContext ctx = recoveryContextWithParquetFiles(
            List.of("_parquet_file_gen_1.parquet"),
            new String[] { "_parquet_file_gen_1.parquet" }
        );
        RecoveryValidationException e = expectThrows(RecoveryValidationException.class, () -> coordinator.onRecovery(state, ctx));
        assertTrue(e.getMessage(), e.getMessage().contains("parquet file too small"));
    }

    public void testOnRecoveryFailsOnShortState() {
        byte[] tooShort = new byte[] { 1, 0, 0 };   // 3 bytes instead of 5
        RecoveryContext ctx = recoveryContextWithParquetFiles(List.of(), new String[0]);
        RecoveryValidationException e = expectThrows(RecoveryValidationException.class, () -> coordinator.onRecovery(tooShort, ctx));
        assertTrue(e.getMessage(), e.getMessage().contains("state too short"));
    }

    // --- helpers ---

    private byte[] validParquetBody() {
        byte[] body = new byte[64];
        body[60] = 'P';
        body[61] = 'A';
        body[62] = 'R';
        body[63] = '1';
        return body;
    }

    private void writeParquetFile(String name, byte[] body) throws IOException {
        Files.write(parquetDir.resolve(name), body);
    }

    private byte[] versionedState(byte version, int count) {
        return ByteBuffer.allocate(5).put(version).putInt(count).array();
    }

    private UploadContext uploadContextWithParquetFiles(int fileCount) {
        CatalogSnapshot snapshot = mock("parquet", fileSets(fileCount));
        return new UploadContext(snapshot, new org.apache.lucene.store.ByteBuffersDirectory(), "parquet", null);
    }

    private RecoveryContext recoveryContextWithParquetFiles(List<String> catalogFiles, String[] localFiles) {
        CatalogSnapshot snapshot = mock(
            "parquet",
            List.of(new WriterFileSet(parquetDir.toString(), 1L, java.util.Set.copyOf(catalogFiles), 0L))
        );
        RecoveryOps ops = new TestOps(localFiles);
        return new RecoveryContext(snapshot, new org.apache.lucene.store.ByteBuffersDirectory(), shardDataPath, "parquet", ops);
    }

    private static List<WriterFileSet> fileSets(int count) {
        java.util.Set<String> files = new java.util.LinkedHashSet<>();
        for (int i = 0; i < count; i++) {
            files.add("_parquet_file_gen_" + (i + 1) + ".parquet");
        }
        return List.of(new WriterFileSet("/tmp/parquet", 1L, files, 0L));
    }

    private static CatalogSnapshot mock(String formatName, List<WriterFileSet> fileSets) {
        return new org.opensearch.index.engine.exec.coord.SegmentInfosCatalogSnapshot(
            new org.apache.lucene.index.SegmentInfos(org.apache.lucene.util.Version.LATEST.major)
        ) {
            @Override
            public java.util.Collection<WriterFileSet> getSearchableFiles(String df) {
                return formatName.equals(df) ? fileSets : List.of();
            }
        };
    }

    /** Minimal RecoveryOps that serves listFormatFiles from a preset array; everything else is no-op. */
    private static final class TestOps implements RecoveryOps {
        private final String[] files;

        TestOps(String[] files) {
            this.files = files;
        }

        @Override
        public void writeSegmentsN(org.apache.lucene.index.SegmentInfos infos, long localCheckpoint) {}

        @Override
        public void deleteStaleSegmentsFiles() {}

        @Override
        public String[] listFormatFiles(String formatName) {
            return files;
        }
    }
}
