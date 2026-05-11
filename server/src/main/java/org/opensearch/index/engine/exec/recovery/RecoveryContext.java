/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.recovery;

import org.apache.lucene.store.Directory;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.nio.file.Path;
import java.util.Objects;

/**
 * Immutable view passed to {@link FormatRecoveryCoordinator#onRecovery}. Carries data plus
 * the narrow {@link RecoveryOps} mutation surface. Coordinators never get raw {@code Store}
 * or {@code IndexShard} access.
 */
@ExperimentalApi
public final class RecoveryContext {

    private final CatalogSnapshot snapshot;
    private final Directory storeDirectory;
    private final Path shardDataPath;
    private final String formatName;
    private final RecoveryOps ops;

    public RecoveryContext(CatalogSnapshot snapshot, Directory storeDirectory, Path shardDataPath, String formatName, RecoveryOps ops) {
        this.snapshot = Objects.requireNonNull(snapshot, "snapshot");
        this.storeDirectory = Objects.requireNonNull(storeDirectory, "storeDirectory");
        this.shardDataPath = Objects.requireNonNull(shardDataPath, "shardDataPath");
        this.formatName = Objects.requireNonNull(formatName, "formatName");
        this.ops = Objects.requireNonNull(ops, "ops");
    }

    public CatalogSnapshot snapshot() {
        return snapshot;
    }

    public Directory storeDirectory() {
        return storeDirectory;
    }

    public Path shardDataPath() {
        return shardDataPath;
    }

    public String formatName() {
        return formatName;
    }

    public RecoveryOps ops() {
        return ops;
    }
}
