/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.recovery;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;

/**
 * Per-format hook for the remote-store recovery protocol. Each data format plugin that
 * needs to contribute state (for file reference tracking, integrity validation, or on-disk
 * reconstruction) provides one implementation.
 *
 * <p><b>Lifecycle:</b> singleton, stateless, thread-safe. Registered once at node boot via
 * {@code DataFormatPlugin.getRecoveryCoordinator()}. Methods may run concurrently for
 * different shards on the same node.
 *
 * <p><b>Failure:</b> {@link #onRecovery} throwing {@link RecoveryValidationException} fails
 * the shard's recovery attempt; the cluster allocator retries. Coordinator writes MUST be
 * idempotent or atomic — partial failed writes must not accumulate across retries.
 */
@ExperimentalApi
public interface FormatRecoveryCoordinator {

    /** Data format name, e.g. {@code "lucene"}, {@code "parquet"}. */
    String formatName();

    /**
     * Upload hook. Called during remote-store metadata upload. Returned bytes are stored
     * verbatim alongside the catalog and handed back to {@link #onRecovery}. Return
     * {@code null} to opt out of the round-trip for this upload.
     *
     * <p>MUST NOT mutate the store or block indefinitely. SHOULD complete within the
     * refresh budget (~100ms typical).
     */
    byte[] captureForUpload(UploadContext context) throws IOException;

    /**
     * Recovery hook. Called after remote files are downloaded and the local catalog is
     * reconstructed. The coordinator validates its files, reconstructs any format-specific
     * on-disk structures, and fails fast on unrecoverable corruption.
     *
     * @param state bytes previously returned by {@link #captureForUpload}; never {@code null}
     * @param context recovery view + narrow {@link RecoveryOps}
     */
    void onRecovery(byte[] state, RecoveryContext context) throws IOException;
}
