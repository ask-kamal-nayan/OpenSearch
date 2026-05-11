/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.Segment;

import java.util.List;

/**
 * Result of a refresh operation containing the refreshed segments and, when applicable, the
 * Lucene coordinator state bytes (pre-packaged in the Lucene recovery coordinator's state
 * layout) captured atomically at the same point in time.
 *
 * <p>The bytes are in the format {@code [generation:long][SegmentInfos bytes with footer
 * checksum]} — a single IndexOutput produced them so the CRC32 footer covers the whole
 * blob. Readers MUST treat these bytes as opaque and pass them through verbatim to avoid
 * breaking checksum validation.
 *
 * @param refreshedSegments        the segments produced by the refresh
 * @param luceneSegmentInfosBytes  pre-packaged Lucene coordinator state bytes captured at
 *                                 the same instant as {@code refreshedSegments}; {@code null}
 *                                 for non-Lucene refreshes (e.g. parquet-only)
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public record RefreshResult(List<Segment> refreshedSegments, byte[] luceneSegmentInfosBytes) {

    /** Convenience constructor for non-Lucene refreshes. */
    public RefreshResult(List<Segment> refreshedSegments) {
        this(refreshedSegments, null);
    }
}
