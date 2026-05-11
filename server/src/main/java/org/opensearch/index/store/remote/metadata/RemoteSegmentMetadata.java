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
import org.apache.lucene.util.Version;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Metadata object for Remote Segment
 *
 * @opensearch.api
 */
@PublicApi(since = "2.6.0")
public class RemoteSegmentMetadata {

    public static final int VERSION_ONE = 1;

    public static final int VERSION_TWO = 2;

    /**
     * v3 adds {@link DfaRecoveryPayload} — typed recovery-time state for DataFormat-aware indices.
     * Non-DFA v3 metadata carries a {@code 0} presence flag and behaves identically to v1/v2 on the
     * read path.
     */
    public static final int VERSION_THREE = 3;

    /**
     * Latest supported version of metadata
     */
    public static final int CURRENT_VERSION = VERSION_THREE;

    /**
     * Metadata codec
     */
    public static final String METADATA_CODEC = "segment_md";

    /**
     * Data structure holding metadata content
     */
    private final Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> metadata;

    private final byte[] segmentInfosBytes;

    private final ReplicationCheckpoint replicationCheckpoint;

    /**
     * Typed DFA payload carrying serialized catalog + per-format recovery state. {@code null}
     * for non-DFA indices and for v1/v2 legacy metadata.
     */
    private final DfaRecoveryPayload dfaPayload;

    /**
     * Backwards-compatible constructor — produces metadata with no DFA payload (i.e. a non-DFA
     * Lucene-only upload). Existing non-DFA callers need no changes.
     */
    public RemoteSegmentMetadata(
        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> metadata,
        byte[] segmentInfosBytes,
        ReplicationCheckpoint replicationCheckpoint
    ) {
        this(metadata, segmentInfosBytes, replicationCheckpoint, null);
    }

    /**
     * Full constructor. Pass a non-null {@code dfaPayload} for DataFormat-aware uploads.
     * For DFA, callers should pass empty {@code segmentInfosBytes}.
     */
    public RemoteSegmentMetadata(
        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> metadata,
        byte[] segmentInfosBytes,
        ReplicationCheckpoint replicationCheckpoint,
        DfaRecoveryPayload dfaPayload
    ) {
        this.metadata = metadata;
        this.segmentInfosBytes = segmentInfosBytes;
        this.replicationCheckpoint = replicationCheckpoint;
        this.dfaPayload = dfaPayload;
    }

    /**
     * Exposes underlying metadata content data structure.
     * @return {@code metadata}
     */
    public Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> getMetadata() {
        return this.metadata;
    }

    public byte[] getSegmentInfosBytes() {
        return segmentInfosBytes;
    }

    /**
     * Typed DataFormat-aware recovery payload. {@code null} for non-DFA indices and for v1/v2
     * legacy bytes. Presence of a non-null payload is the canonical "is this a DFA index?" signal
     * on the wire.
     */
    public DfaRecoveryPayload getDfaPayload() {
        return dfaPayload;
    }

    public long getGeneration() {
        return replicationCheckpoint.getSegmentsGen();
    }

    public long getPrimaryTerm() {
        return replicationCheckpoint.getPrimaryTerm();
    }

    public ReplicationCheckpoint getReplicationCheckpoint() {
        return replicationCheckpoint;
    }

    public Map<String, String> toMapOfStrings() {
        return this.metadata.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().toString()));
    }

    public static Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> fromMapOfStrings(Map<String, String> segmentMetadata) {
        return segmentMetadata.entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> RemoteSegmentStoreDirectory.UploadedSegmentMetadata.fromString(entry.getValue())
                )
            );
    }

    /**
     * Writes at {@link #CURRENT_VERSION}. On v3 a trailing presence byte indicates whether a
     * typed {@link DfaRecoveryPayload} follows.
     */
    public void write(IndexOutput out) throws IOException {
        out.writeMapOfStrings(toMapOfStrings());
        writeCheckpointToIndexOutput(replicationCheckpoint, out);
        out.writeLong(segmentInfosBytes.length);
        out.writeBytes(segmentInfosBytes, segmentInfosBytes.length);
        // v3+: trailing optional typed payload. Single presence byte keeps the footer compact.
        if (dfaPayload != null) {
            out.writeByte((byte) 1);
            dfaPayload.writeTo(out);
        } else {
            out.writeByte((byte) 0);
        }
    }

    /**
     * Reads metadata of a supported {@code version}. v1/v2 produce {@code dfaPayload == null};
     * v3 may carry a payload depending on the presence byte.
     */
    public static RemoteSegmentMetadata read(IndexInput indexInput, int version) throws IOException {
        Map<String, String> metadata = indexInput.readMapOfStrings();
        final Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> uploadedSegmentMetadataMap = RemoteSegmentMetadata
            .fromMapOfStrings(metadata);
        ReplicationCheckpoint replicationCheckpoint = readCheckpointFromIndexInput(indexInput, uploadedSegmentMetadataMap, version);
        int byteArraySize = (int) indexInput.readLong();
        byte[] segmentInfosBytes = new byte[byteArraySize];
        indexInput.readBytes(segmentInfosBytes, 0, byteArraySize);

        DfaRecoveryPayload dfaPayload = null;
        if (version >= VERSION_THREE) {
            byte presence = indexInput.readByte();
            if (presence == 1) {
                dfaPayload = DfaRecoveryPayload.readFrom(indexInput);
            } else if (presence != 0) {
                throw new IOException("Invalid DfaRecoveryPayload presence flag: " + presence);
            }
        }
        return new RemoteSegmentMetadata(uploadedSegmentMetadataMap, segmentInfosBytes, replicationCheckpoint, dfaPayload);
    }

    public static void writeCheckpointToIndexOutput(ReplicationCheckpoint replicationCheckpoint, IndexOutput out) throws IOException {
        ShardId shardId = replicationCheckpoint.getShardId();
        // Write ShardId
        out.writeString(shardId.getIndex().getName());
        out.writeString(shardId.getIndex().getUUID());
        out.writeVInt(shardId.getId());
        // Write remaining checkpoint fields
        out.writeLong(replicationCheckpoint.getPrimaryTerm());
        out.writeLong(replicationCheckpoint.getSegmentsGen());
        out.writeLong(replicationCheckpoint.getSegmentInfosVersion());
        out.writeLong(replicationCheckpoint.getLength());
        out.writeString(replicationCheckpoint.getCodec());
        out.writeLong(replicationCheckpoint.getCreatedTimeStamp());
    }

    private static ReplicationCheckpoint readCheckpointFromIndexInput(
        IndexInput in,
        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> uploadedSegmentMetadataMap,
        int version
    ) throws IOException {
        return new ReplicationCheckpoint(
            new ShardId(new Index(in.readString(), in.readString()), in.readVInt()),
            in.readLong(),
            in.readLong(),
            in.readLong(),
            in.readLong(),
            in.readString(),
            toStoreFileMetadata(uploadedSegmentMetadataMap),
            version >= VERSION_TWO ? in.readLong() : 0
        );
    }

    private static Map<String, StoreFileMetadata> toStoreFileMetadata(
        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> metadata
    ) {
        return metadata.entrySet()
            .stream()
            // TODO: Version here should be read from UploadedSegmentMetadata.
            .map(
                entry -> new StoreFileMetadata(entry.getKey(), entry.getValue().getLength(), entry.getValue().getChecksum(), Version.LATEST)
            )
            .collect(Collectors.toMap(StoreFileMetadata::name, Function.identity()));
    }
}
