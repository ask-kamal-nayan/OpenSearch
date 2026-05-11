/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.metadata;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Typed wire payload carrying DataFormat-aware recovery state as part of {@link RemoteSegmentMetadata}
 * version 3.
 *
 * <p>Replaces the Phase 2 approach of tunneling the catalog through a synthetic Lucene
 * {@link org.apache.lucene.index.SegmentInfos} envelope's {@code userData}. Here the catalog
 * bytes and per-format states are first-class wire fields.
 *
 * <p><b>Contents:</b>
 * <ul>
 *   <li>{@code catalogSnapshotBytes} — raw serialized {@link
 *       org.opensearch.index.engine.exec.coord.DataformatAwareCatalogSnapshot} (no Lucene framing).</li>
 *   <li>{@code lastCommitGeneration} — explicit commit generation (previously inferred from
 *       {@code SegmentInfos}).</li>
 *   <li>{@code formatStates} — per-format opaque bytes produced by
 *       {@code FormatRecoveryCoordinator.captureForUpload}.</li>
 * </ul>
 *
 * <p><b>Evolution:</b> fields are written in a fixed order with length prefixes so readers can
 * skip over unknown tail bytes. Future additions should be appended at the tail using the same
 * length-prefixed pattern; older v3 readers will naturally ignore them without a wire-version
 * bump. Only a semantic breaking change (removing/renaming a field) requires v4.
 *
 * <p>Immutable and thread-safe. Constructed once at upload time, read-only thereafter.
 */
@ExperimentalApi
public final class DfaRecoveryPayload {

    /**
     * Format of the embedded byte layout this payload was written with. Distinct from
     * {@link RemoteSegmentMetadata#CURRENT_VERSION} which tracks the outer metadata format.
     * Present to allow internal evolution of the payload layout within a single metadata version.
     */
    public static final int PAYLOAD_VERSION_ONE = 1;

    /** Magic prefix to catch mis-reads (e.g. reading v1/v2 bytes as DFA payload). */
    private static final int MAGIC = 0x44464150; // "DFAP"

    private final int payloadVersion;
    private final byte[] catalogSnapshotBytes;
    private final long lastCommitGeneration;
    private final Map<String, byte[]> formatStates;

    public DfaRecoveryPayload(byte[] catalogSnapshotBytes, long lastCommitGeneration, Map<String, byte[]> formatStates) {
        this(PAYLOAD_VERSION_ONE, catalogSnapshotBytes, lastCommitGeneration, formatStates);
    }

    private DfaRecoveryPayload(
        int payloadVersion,
        byte[] catalogSnapshotBytes,
        long lastCommitGeneration,
        Map<String, byte[]> formatStates
    ) {
        this.payloadVersion = payloadVersion;
        this.catalogSnapshotBytes = Objects.requireNonNull(catalogSnapshotBytes, "catalogSnapshotBytes");
        this.lastCommitGeneration = lastCommitGeneration;
        this.formatStates = Map.copyOf(Objects.requireNonNull(formatStates, "formatStates"));
    }

    public int getPayloadVersion() {
        return payloadVersion;
    }

    public byte[] getCatalogSnapshotBytes() {
        return catalogSnapshotBytes;
    }

    public long getLastCommitGeneration() {
        return lastCommitGeneration;
    }

    public Map<String, byte[]> getFormatStates() {
        return formatStates;
    }

    /**
     * Serializes the payload to the given output. Layout:
     * <pre>
     *   int   magic = 0x44464150
     *   vint  payloadVersion
     *   vint  catalogSnapshotBytes.length
     *   bytes catalogSnapshotBytes
     *   long  lastCommitGeneration
     *   vint  formatStates.size()
     *   for each: string key, vint value.length, bytes value
     * </pre>
     */
    public void writeTo(IndexOutput out) throws IOException {
        out.writeInt(MAGIC);
        out.writeVInt(payloadVersion);
        out.writeVInt(catalogSnapshotBytes.length);
        out.writeBytes(catalogSnapshotBytes, catalogSnapshotBytes.length);
        out.writeLong(lastCommitGeneration);
        out.writeVInt(formatStates.size());
        for (Map.Entry<String, byte[]> e : formatStates.entrySet()) {
            out.writeString(e.getKey());
            byte[] v = e.getValue();
            out.writeVInt(v.length);
            out.writeBytes(v, v.length);
        }
    }

    /**
     * Reads a payload from the given input. Throws {@link IOException} if the magic is wrong,
     * which guards against accidentally reading non-DFA bytes as a DFA payload.
     */
    public static DfaRecoveryPayload readFrom(IndexInput in) throws IOException {
        int magic = in.readInt();
        if (magic != MAGIC) {
            throw new IOException(
                "Invalid DfaRecoveryPayload magic: expected 0x" + Integer.toHexString(MAGIC) + " got 0x" + Integer.toHexString(magic)
            );
        }
        int payloadVersion = in.readVInt();
        if (payloadVersion != PAYLOAD_VERSION_ONE) {
            throw new IOException("Unsupported DfaRecoveryPayload version: " + payloadVersion);
        }
        int catalogLen = in.readVInt();
        byte[] catalogBytes = new byte[catalogLen];
        in.readBytes(catalogBytes, 0, catalogLen);
        long lastCommitGeneration = in.readLong();
        int mapSize = in.readVInt();
        Map<String, byte[]> states = new HashMap<>(mapSize);
        for (int i = 0; i < mapSize; i++) {
            String key = in.readString();
            int vLen = in.readVInt();
            byte[] value = new byte[vLen];
            in.readBytes(value, 0, vLen);
            states.put(key, value);
        }
        return new DfaRecoveryPayload(payloadVersion, catalogBytes, lastCommitGeneration, states);
    }
}
