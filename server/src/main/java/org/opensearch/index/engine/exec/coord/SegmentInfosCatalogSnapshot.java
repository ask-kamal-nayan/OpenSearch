/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class SegmentInfosCatalogSnapshot extends CatalogSnapshot {

    private static final String CATALOG_SNAPSHOT_KEY = "_segment_infos_catalog_snapshot_";

    private final SegmentInfos segmentInfos;

    public SegmentInfosCatalogSnapshot(SegmentInfos segmentInfos) {
        super(CATALOG_SNAPSHOT_KEY + segmentInfos.getGeneration(), segmentInfos.getGeneration(), segmentInfos.getVersion());
        this.segmentInfos = segmentInfos;
    }

    public SegmentInfosCatalogSnapshot(StreamInput in) throws IOException {
        super(in);
        byte[] segmentInfosBytes = in.readByteArray();
        this.segmentInfos = SegmentInfos.readCommit(
            null,
            new BufferedChecksumIndexInput(new ByteArrayIndexInput("SegmentInfos", segmentInfosBytes)),
            0L
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        ByteBuffersDataOutput buffer = new ByteBuffersDataOutput();
        try (ByteBuffersIndexOutput indexOutput = new ByteBuffersIndexOutput(buffer, "", null)) {
            segmentInfos.write(indexOutput);
        }
        out.writeByteArray(buffer.toArrayCopy());
    }

    @Override
    public Collection<FileMetadata> getFileMetadataList() throws IOException {
        return segmentInfos.files(true).stream().map(file -> new FileMetadata("lucene", file)).collect(Collectors.toList());
    }

    public SegmentInfos getSegmentInfos() {
        return segmentInfos;
    }

    @Override
    public Map<String, String> getUserData() {
        return segmentInfos.getUserData();
    }

    @Override
    public long getId() {
        return generation;
    }

    @Override
    public List<Segment> getSegments() {
        throw new UnsupportedOperationException("SegmentInfosCatalogSnapshot does not support getSegments()");
    }

    @Override
    public Collection<WriterFileSet> getSearchableFiles(String dataFormat) {
        throw new UnsupportedOperationException("SegmentInfosCatalogSnapshot does not support getSearchableFiles()");
    }

    @Override
    public Set<String> getDataFormats() {
        throw new UnsupportedOperationException("SegmentInfosCatalogSnapshot does not support getDataFormats()");
    }

    @Override
    public long getLastWriterGeneration() {
        return -1;
    }

    @Override
    public String serializeToString() throws IOException {
        throw new UnsupportedOperationException("SegmentInfosCatalogSnapshot does not support serializeToString()");
    }

    @Override
    public void remapPaths(Path newShardDataPath) {
        // No-op for SegmentInfosCatalogSnapshot
    }

    @Override
    public void setIndexFileDeleterSupplier(java.util.function.Supplier<IndexFileDeleter> supplier) {
        // No-op for SegmentInfosCatalogSnapshot
    }

    @Override
    public void setCatalogSnapshotMap(Map<Long, ? extends CatalogSnapshot> catalogSnapshotMap) {
        // No-op for SegmentInfosCatalogSnapshot
    }

    @Override
    public SegmentInfosCatalogSnapshot clone() {
        return new SegmentInfosCatalogSnapshot(segmentInfos);
    }

    @Override
    protected void closeInternal() {
        // TODO no op since SegmentInfosCatalogSnapshot is not refcounted
    }

    @Override
    public void setUserData(Map<String, String> userData, boolean b) {
        // Update the internal SegmentInfos userData so callers don't need instanceof checks
        segmentInfos.setUserData(userData, b);
    }

    /**
     * Returns the Lucene major version that wrote the given segment file by looking it up
     * from the SegmentInfos. Falls back to the SegmentInfos commit version for the segments
     * file itself, or to the .si file's version for other unmapped files.
     */
    @Override
    public int getLuceneVersionForFile(String file) {
        Map<String, Integer> versionMap = buildSegmentToLuceneVersionMap();
        Integer version = versionMap.get(file);
        if (version != null) {
            return version;
        }
        // Check if this is the segments file itself
        if (file.equals(segmentInfos.getSegmentsFileName())) {
            return segmentInfos.getCommitLuceneVersion().major;
        }
        // Fallback to the Lucene major version of the respective segment's .si file
        String segmentInfoFileName = RemoteStoreUtils.getSegmentName(file) + ".si";
        Integer siVersion = versionMap.get(segmentInfoFileName);
        if (siVersion != null) {
            return siVersion;
        }
        return org.apache.lucene.util.Version.LATEST.major;
    }

    /**
     * Serializes the actual SegmentInfos to bytes for the remote metadata file.
     */
    @Override
    public byte[] serializeToSegmentInfosBytes(ReplicationCheckpoint replicationCheckpoint) throws IOException {
        ByteBuffersDataOutput byteBuffersIndexOutput = new ByteBuffersDataOutput();
        segmentInfos.write(
            new ByteBuffersIndexOutput(byteBuffersIndexOutput, "Snapshot of SegmentInfos", "SegmentInfos")
        );
        return byteBuffersIndexOutput.toArrayCopy();
    }

    /**
     * Builds a map of segment file name to the Lucene major version that wrote it,
     * by walking the SegmentInfos.
     */
    private Map<String, Integer> buildSegmentToLuceneVersionMap() {
        Map<String, Integer> segmentToLuceneVersion = new HashMap<>();
        for (SegmentCommitInfo segmentCommitInfo : segmentInfos) {
            SegmentInfo info = segmentCommitInfo.info;
            Set<String> segFiles = info.files();
            for (String segFile : segFiles) {
                segmentToLuceneVersion.put(segFile, info.getVersion().major);
            }
        }
        return segmentToLuceneVersion;
    }
}
