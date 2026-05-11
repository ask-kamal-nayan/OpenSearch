/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.recovery;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfos;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.Store;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Default {@link RecoveryOps} implementation backed by the shard's {@link Store} and
 * {@link ShardPath}. Exposes the minimum surface coordinators need during recovery.
 */
@ExperimentalApi
public final class StoreBackedRecoveryOps implements RecoveryOps {

    private final Store store;
    private final ShardPath shardPath;

    public StoreBackedRecoveryOps(Store store, ShardPath shardPath) {
        this.store = Objects.requireNonNull(store, "store");
        this.shardPath = Objects.requireNonNull(shardPath, "shardPath");
    }

    @Override
    public void writeSegmentsN(SegmentInfos infos, long localCheckpoint) throws IOException {
        store.commitSegmentInfos(infos, localCheckpoint, localCheckpoint);
    }

    @Override
    public void deleteStaleSegmentsFiles() throws IOException {
        String[] existing = store.directory().listAll();
        for (String name : existing) {
            if (name.startsWith(IndexFileNames.SEGMENTS)) {
                store.deleteQuiet(name);
            }
        }
    }

    @Override
    public String[] listFormatFiles(String formatName) throws IOException {
        Path formatDir = shardPath.getDataPath().resolve(formatName);
        if (Files.isDirectory(formatDir) == false) {
            return new String[0];
        }
        try (Stream<Path> s = Files.list(formatDir)) {
            return s.map(p -> p.getFileName().toString()).sorted().toArray(String[]::new);
        }
    }
}
