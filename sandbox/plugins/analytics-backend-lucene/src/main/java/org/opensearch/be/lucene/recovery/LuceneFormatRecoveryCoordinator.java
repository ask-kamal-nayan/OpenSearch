/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.recovery;

import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.opensearch.be.lucene.index.LuceneCommitter;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.recovery.FormatRecoveryCoordinator;
import org.opensearch.index.engine.exec.recovery.RecoveryContext;
import org.opensearch.index.engine.exec.recovery.RecoveryValidationException;
import org.opensearch.index.engine.exec.recovery.UploadContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Lucene's implementation of {@link FormatRecoveryCoordinator}. Captures the
 * {@code IndexWriter}'s in-memory {@link SegmentInfos} at upload time and re-materializes
 * {@code segments_N} at recovery time so the Lucene {@code IndexFileDeleter} does not
 * treat on-disk Lucene files as unreferenced.
 *
 * <p>Stateless singleton; registered by {@code LucenePlugin.getRecoveryCoordinator()}.
 */
@ExperimentalApi
public final class LuceneFormatRecoveryCoordinator implements FormatRecoveryCoordinator {

    public static final String FORMAT_NAME = "lucene";

    @Override
    public String formatName() {
        return FORMAT_NAME;
    }

    @Override
    public byte[] captureForUpload(UploadContext context) throws IOException {
        // Prefer bytes captured atomically with the catalog during refresh. The bytes are
        // already in this coordinator's state layout — [generation:long][SegmentInfos bytes
        // with footer checksum] — so return them verbatim. Re-wrapping in a fresh IndexOutput
        // would break the checksum.
        if (context.snapshot() instanceof org.opensearch.index.engine.exec.coord.DataformatAwareCatalogSnapshot dfa) {
            byte[] atomic = dfa.getLuceneSegmentInfosBytes();
            if (atomic != null) {
                return atomic;
            }
        }

        // Startup / edge-case fallback: no atomic bytes available (catalog reloaded from disk
        // before any refresh). Fall back to a fresh committer capture — safe here because
        // IndexWriter state matches the on-disk commit at this point.
        Optional<Committer> committer = context.committer();
        if (committer.isEmpty() || committer.get() instanceof LuceneCommitter == false) {
            return null;  // nothing to contribute; upload will carry no Lucene state for this shard
        }
        LuceneCommitter luceneCommitter = (LuceneCommitter) committer.get();
        SegmentInfos infos = luceneCommitter.captureInMemorySegmentInfos();
        if (infos == null) {
            return null;
        }
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        ByteBuffersIndexOutput idx = new ByteBuffersIndexOutput(out, "lucene-upload-state", "lucene-upload-state");
        idx.writeLong(infos.getGeneration());
        infos.write(idx);
        return out.toArrayCopy();
    }

    @Override
    public void onRecovery(byte[] state, RecoveryContext context) throws IOException {
        SegmentInfos infos;
        try (ChecksumIndexInput in = toChecksumInput(state)) {
            long generation = in.readLong();
            infos = SegmentInfos.readCommit(context.storeDirectory(), in, generation);
        } catch (IOException e) {
            throw RecoveryValidationException.wrap(formatName(), e);
        }

        // Merge the DFA catalog's userData AND the serialized catalog snapshot itself into the
        // decoded SegmentInfos. The catalog snapshot string under CATALOG_SNAPSHOT_KEY is what
        // future readers (engine open, Store.fromSegmentInfos) use to reconstruct the DFA catalog
        // from the committed segments_N.
        Map<String, String> userData = new HashMap<>(infos.getUserData());
        userData.putAll(context.snapshot().getUserData());
        userData.put(CatalogSnapshot.CATALOG_SNAPSHOT_KEY, context.snapshot().serializeToString());
        infos.setUserData(userData, false);

        long localCheckpoint = parseLongOrDefault(userData, "local_checkpoint", -1);
        context.ops().deleteStaleSegmentsFiles();
        context.ops().writeSegmentsN(infos, localCheckpoint);
    }

    private static ChecksumIndexInput toChecksumInput(byte[] bytes) {
        return new BufferedChecksumIndexInput(new ByteArrayInput(bytes));
    }

    private static long parseLongOrDefault(Map<String, String> userData, String key, long fallback) {
        String v = userData.get(key);
        if (v == null || v.isEmpty()) return fallback;
        try {
            return Long.parseLong(v);
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    /**
     * Minimal {@link IndexInput} over a byte array. Used to wrap the upload-state bytes
     * for {@link SegmentInfos#readCommit}.
     */
    private static final class ByteArrayInput extends IndexInput {
        private final ByteArrayDataInput in;
        private final int length;

        ByteArrayInput(byte[] bytes) {
            super("lucene-upload-state");
            this.in = new ByteArrayDataInput(bytes);
            this.length = bytes.length;
        }

        @Override
        public void close() {}

        @Override
        public long getFilePointer() {
            return in.getPosition();
        }

        @Override
        public void seek(long pos) {
            in.setPosition(Math.toIntExact(pos));
        }

        @Override
        public long length() {
            return length;
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) {
            throw new UnsupportedOperationException("slice");
        }

        @Override
        public byte readByte() {
            return in.readByte();
        }

        @Override
        public void readBytes(byte[] b, int off, int len) {
            in.readBytes(b, off, len);
        }

        @Override
        public IndexInput clone() {
            return this;  // immutable view; safe to share
        }
    }
}
