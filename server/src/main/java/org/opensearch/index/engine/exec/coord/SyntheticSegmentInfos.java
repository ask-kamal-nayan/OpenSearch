/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Builds the synthetic Lucene {@code SegmentInfos} payload uploaded by the DFA primary. Zero
 * segment entries — a transport envelope carrying the serialized {@link
 * DataformatAwareCatalogSnapshot} in {@code userData}.
 *
 * <p>Real Lucene segment references travel through the format-recovery protocol in
 * {@code RemoteSegmentMetadata.formatStates["lucene"]}, populated by
 * {@code LuceneFormatRecoveryCoordinator}. See
 * {@code docs/design/multi-dataformat-recovery-validation.md}.
 */
final class SyntheticSegmentInfos {

    private SyntheticSegmentInfos() {}

    /**
     * Builds envelope bytes with the catalog serialized into {@code userData} under
     * {@link CatalogSnapshot#CATALOG_SNAPSHOT_KEY}.
     */
    static byte[] serialize(DataformatAwareCatalogSnapshot snapshot) throws IOException {
        SegmentInfos segmentInfos = new SegmentInfos(Version.LATEST.major);
        Map<String, String> userData = new HashMap<>(snapshot.getUserData());
        userData.put(CatalogSnapshot.CATALOG_SNAPSHOT_KEY, snapshot.serializeToString());
        segmentInfos.setUserData(userData, false);
        segmentInfos.setNextWriteGeneration(snapshot.getLastCommitGeneration());

        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        segmentInfos.write(new ByteBuffersIndexOutput(out, "DFA upload SegmentInfos", "DFA upload SegmentInfos"));
        return out.toArrayCopy();
    }
}
