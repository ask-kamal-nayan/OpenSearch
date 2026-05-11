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
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.util.Objects;
import java.util.Optional;

/**
 * Immutable view passed to {@link FormatRecoveryCoordinator#captureForUpload}. Carries
 * data only — no references to {@code IndexShard} or {@code Store}. New fields can be
 * added without breaking existing coordinators.
 */
@ExperimentalApi
public final class UploadContext {

    private final CatalogSnapshot snapshot;
    private final Directory storeDirectory;
    private final String formatName;
    private final Committer committer;

    public UploadContext(CatalogSnapshot snapshot, Directory storeDirectory, String formatName, Committer committer) {
        this.snapshot = Objects.requireNonNull(snapshot, "snapshot");
        this.storeDirectory = Objects.requireNonNull(storeDirectory, "storeDirectory");
        this.formatName = Objects.requireNonNull(formatName, "formatName");
        this.committer = committer;
    }

    public CatalogSnapshot snapshot() {
        return snapshot;
    }

    public Directory storeDirectory() {
        return storeDirectory;
    }

    public String formatName() {
        return formatName;
    }

    /**
     * The engine's committer, if available. Coordinators for formats with a Lucene-style
     * committer can cast and read in-memory state. May be {@link Optional#empty()} for
     * formats that don't use a committer.
     */
    public Optional<Committer> committer() {
        return Optional.ofNullable(committer);
    }
}
