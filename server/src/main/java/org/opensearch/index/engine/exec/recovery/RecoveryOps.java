/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.recovery;

import org.apache.lucene.index.SegmentInfos;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;

/**
 * Narrow mutation surface exposed to a {@link FormatRecoveryCoordinator} during recovery.
 * Deliberately does not expose raw {@code Store} / {@code IndexShard} — coordinators only
 * get the operations they need.
 */
@ExperimentalApi
public interface RecoveryOps {

    /**
     * Atomically writes a Lucene {@code segments_N} from the given {@link SegmentInfos}.
     * Idempotent with respect to repeat attempts.
     */
    void writeSegmentsN(SegmentInfos infos, long localCheckpoint) throws IOException;

    /** Removes stale {@code segments_*} files in the shard's main Lucene index directory. */
    void deleteStaleSegmentsFiles() throws IOException;

    /** Lists files in a format-specific subdirectory, e.g. {@code <shard>/parquet/}. */
    String[] listFormatFiles(String formatName) throws IOException;

    /**
     * Fails the current recovery attempt with a {@link RecoveryValidationException}.
     * Prefer this over throwing directly so error messages are consistent.
     */
    default void failRecovery(String formatName, String reason, Throwable cause) {
        throw new RecoveryValidationException(formatName, reason, cause);
    }
}
