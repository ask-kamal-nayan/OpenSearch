/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.metadata;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DfaRecoveryPayloadTests extends OpenSearchTestCase {

    public void testRoundTripEmptyFormatStates() throws IOException {
        byte[] catalog = new byte[] { 1, 2, 3, 4, 5 };
        DfaRecoveryPayload payload = new DfaRecoveryPayload(catalog, 7L, Map.of());

        byte[] serialized = writeToBytes(payload);
        DfaRecoveryPayload round = readFromBytes(serialized);

        assertArrayEquals(catalog, round.getCatalogSnapshotBytes());
        assertEquals(7L, round.getLastCommitGeneration());
        assertTrue(round.getFormatStates().isEmpty());
        assertEquals(DfaRecoveryPayload.PAYLOAD_VERSION_ONE, round.getPayloadVersion());
    }

    public void testRoundTripWithMultipleFormatStates() throws IOException {
        byte[] catalog = new byte[] { 0x10, 0x20, 0x30 };
        Map<String, byte[]> states = new HashMap<>();
        states.put("lucene", new byte[] { 'L', 'U', 'C' });
        states.put("parquet", new byte[] { 'P', 'A', 'R', '1' });
        states.put("future_format", new byte[0]);

        DfaRecoveryPayload payload = new DfaRecoveryPayload(catalog, 42L, states);
        DfaRecoveryPayload round = readFromBytes(writeToBytes(payload));

        assertArrayEquals(catalog, round.getCatalogSnapshotBytes());
        assertEquals(42L, round.getLastCommitGeneration());
        assertEquals(3, round.getFormatStates().size());
        assertArrayEquals(new byte[] { 'L', 'U', 'C' }, round.getFormatStates().get("lucene"));
        assertArrayEquals(new byte[] { 'P', 'A', 'R', '1' }, round.getFormatStates().get("parquet"));
        assertArrayEquals(new byte[0], round.getFormatStates().get("future_format"));
    }

    public void testBadMagicRejected() throws IOException {
        // Write 4 bytes of garbage then attempt to read as a payload — must fail with IOException.
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        try (IndexOutput o = new ByteBuffersIndexOutput(out, "test", "test")) {
            o.writeInt(0xDEADBEEF);
        }
        IOException ex = expectThrows(IOException.class, () -> readFromBytes(out.toArrayCopy()));
        assertTrue(ex.getMessage().contains("magic"));
    }

    public void testImmutableFormatStates() {
        Map<String, byte[]> states = new HashMap<>();
        states.put("lucene", new byte[] { 1 });
        DfaRecoveryPayload payload = new DfaRecoveryPayload(new byte[] { 0 }, 0L, states);

        // Mutating the input map must not affect the payload's view.
        states.put("added_later", new byte[] { 99 });
        assertEquals(1, payload.getFormatStates().size());

        // Payload's map must itself be unmodifiable.
        expectThrows(UnsupportedOperationException.class, () -> payload.getFormatStates().put("x", new byte[0]));
    }

    public void testNullFieldsRejected() {
        expectThrows(NullPointerException.class, () -> new DfaRecoveryPayload(null, 0L, Map.of()));
        expectThrows(NullPointerException.class, () -> new DfaRecoveryPayload(new byte[0], 0L, null));
    }

    private static byte[] writeToBytes(DfaRecoveryPayload payload) throws IOException {
        ByteBuffersDataOutput buf = new ByteBuffersDataOutput();
        try (IndexOutput out = new ByteBuffersIndexOutput(buf, "test", "test")) {
            payload.writeTo(out);
        }
        return buf.toArrayCopy();
    }

    private static DfaRecoveryPayload readFromBytes(byte[] bytes) throws IOException {
        try (IndexInput in = new ByteArrayIndexInput(bytes)) {
            return DfaRecoveryPayload.readFrom(in);
        }
    }

    /** Minimal IndexInput over a byte array for test use. */
    private static final class ByteArrayIndexInput extends IndexInput {
        private final ByteArrayDataInput in;
        private final int length;

        ByteArrayIndexInput(byte[] bytes) {
            super("ByteArrayIndexInput");
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
            throw new UnsupportedOperationException();
        }

        @Override
        public byte readByte() {
            return in.readByte();
        }

        @Override
        public void readBytes(byte[] b, int off, int len) {
            in.readBytes(b, off, len);
        }
    }

    @SuppressWarnings("unused")
    private static ChecksumIndexInput unused(ChecksumIndexInput c) {
        return c;
    }
}
