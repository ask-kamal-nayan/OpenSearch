/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Reproduces the race between {@code DataFormatAwareEngine.flush()} and a concurrent
 * refresh that leaves the just-committed {@link CatalogSnapshot} with a stale
 * {@code lastCommitFileName}, and validates that holding {@code refreshLock} across
 * the entire flush operation prevents the race.
 *
 * <p>Without the fix:
 * <pre>
 *   T1 (flush)                                T2 (refresh)
 *   ----------------------                    ----------------------
 *   acquireSnapshotForCommit()
 *     -> snapshot = N (lastCommitFileName="segments_X" inherited)
 *   committer.commit()
 *     -> writes new segments_Y on disk
 *   ## flushReachedRacePointLatch fires ##
 *   await flushRaceReleaseLatch ........
 *                                            engine.refresh("during-flush")
 *                                              -> commitNewSnapshot creates N+1
 *                                              -> N+1.lastCommitFileName="segments_X" (inherited stale)
 *                                              -> latestCatalogSnapshot = N+1
 *   ## release ##
 *   updateLastCommitInfo(commitResult=segments_Y)
 *     -> latestCatalogSnapshot.setLastCommitInfo("segments_Y")
 *     -> latestCatalogSnapshot is N+1, so N+1 gets segments_Y
 *     -> N (the snapshot being committed) keeps its inherited "segments_X"
 *   markSuccess() -> onCommit(N)
 *     -> N enters committedSnapshots with stale lastCommitFileName="segments_X"  ← BUG
 * </pre>
 *
 * <p>With the fix (refreshLock held across flush): the concurrent refresh blocks on
 * refreshLock until flush releases it. updateLastCommitInfo runs against
 * latestCatalogSnapshot=N (still the snapshot we committed), so N gets the correct
 * lastCommitFileName before entering committedSnapshots.
 *
 * <p>The assertion verifies that the just-committed snapshot's {@code lastCommitFileName}
 * matches the actual Lucene segments file produced by the commit.
 */
public class LuceneCommitFileNameRaceTests extends LuceneDataFormatAwareEngineTests {

    public void testCommitFileNameRaceWithConcurrentRefresh() throws Exception {
        try (DataFormatAwareEngine engine = createEngine()) {
            engine.translogManager()
                .recoverFromTranslog(ignore -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);

            // --- baseline flush so we have an initial segments_<X> on disk ---
            for (int i = 0; i < 5; i++) {
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
            }
            engine.refresh("baseline");
            engine.flush(true, true);

            // capture the baseline commit file
            final String baselineCommitFile = readActualSegmentsFile(engine);
            logger.info("[race-test] baseline segments file = {}", baselineCommitFile);

            // index more docs and refresh — this creates a new (uncommitted) snapshot
            // whose lastCommitFileName is INHERITED from the baseline commit
            for (int i = 5; i < 12; i++) {
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
            }
            engine.refresh("pre-race");

            // --- arm race latches ---
            final CountDownLatch flushReached = new CountDownLatch(1);
            final CountDownLatch releaseFlush = new CountDownLatch(1);
            engine.flushReachedRacePointLatch = flushReached;
            engine.flushRaceReleaseLatch = releaseFlush;

            // --- start flush in background; it will pause inside flush() between
            //     committer.commit() and updateLastCommitInfo() ---
            final AtomicReference<Throwable> flushError = new AtomicReference<>();
            Thread flushThread = new Thread(() -> {
                try {
                    engine.flush(true, true);
                } catch (Throwable t) {
                    flushError.set(t);
                }
            }, "race-flush");
            flushThread.start();

            // wait until flush has called committer.commit() and is paused
            assertTrue(
                "flush did not reach the race point within 10s",
                flushReached.await(10, TimeUnit.SECONDS)
            );
            logger.info("[race-test] flush paused after commit(), firing concurrent refresh");

            // --- race window: index more docs, then refresh from a separate thread.
            // With the refreshLock fix in flush(), this refresh blocks on refreshLock until
            // flush completes. Without the fix, it would race and produce stale state.
            for (int i = 12; i < 18; i++) {
                engine.index(indexOp(createParsedDocWithInput(Integer.toString(i), null)));
            }
            final AtomicReference<Throwable> refreshError = new AtomicReference<>();
            Thread refreshThread = new Thread(() -> {
                try {
                    engine.refresh("during-flush");
                } catch (Throwable t) {
                    refreshError.set(t);
                }
            }, "race-refresh");
            refreshThread.start();
            // brief pause to let refreshThread reach refreshLock.lock() (where it will block)
            Thread.sleep(200);
            logger.info("[race-test] concurrent refresh thread is blocked on refreshLock; releasing flush");

            // --- release flush; it now proceeds to updateLastCommitInfo, then releases refreshLock,
            //     unblocking the refresh thread ---
            releaseFlush.countDown();
            flushThread.join(15_000);
            refreshThread.join(15_000);
            assertFalse("flush thread did not finish in 15s", flushThread.isAlive());
            assertFalse("refresh thread did not finish in 15s", refreshThread.isAlive());
            assertNull("flush threw: " + flushError.get(), flushError.get());
            assertNull("refresh threw: " + refreshError.get(), refreshError.get());

            // --- read the actual Lucene segments file the flush wrote ---
            final String actualCommitFileAfterFlush = readActualSegmentsFile(engine);
            logger.info(
                "[race-test] actual segments file after flush = {} (baseline was {})",
                actualCommitFileAfterFlush,
                baselineCommitFile
            );
            assertNotEquals(
                "flush should have written a NEW segments_<X> distinct from the baseline",
                baselineCommitFile,
                actualCommitFileAfterFlush
            );

            // --- the just-committed snapshot's lastCommitFileName must match the new file ---
            try (GatedCloseable<CatalogSnapshot> committedRef = engine.acquireLastCommittedSnapshot(false)) {
                CatalogSnapshot committed = committedRef.get();
                String lastCommitFileName = committed.getLastCommitFileName();
                logger.info(
                    "[race-test] committed snapshot gen={} lastCommitFileName={}",
                    committed.getGeneration(),
                    lastCommitFileName
                );

                assertEquals(
                    "Committed snapshot's lastCommitFileName should match the Lucene segments "
                        + "file produced by the flush. A mismatch indicates the race between "
                        + "commit() and updateLastCommitInfo(): a concurrent refresh advanced "
                        + "latestCatalogSnapshot before updateLastCommitInfo could write to it, "
                        + "so the committed snapshot kept its inherited (stale) value.",
                    actualCommitFileAfterFlush,
                    lastCommitFileName
                );
            } finally {
                // clear test hooks to avoid leaking into inherited tests
                engine.flushReachedRacePointLatch = null;
                engine.flushRaceReleaseLatch = null;
            }
        }
    }

    /** Reads the latest Lucene commit's segments file name directly from the store directory. */
    private static String readActualSegmentsFile(DataFormatAwareEngine engine) throws Exception {
        Directory dir = engine.config().getStore().directory();
        SegmentInfos infos = SegmentInfos.readLatestCommit(dir);
        return infos.getSegmentsFileName();
    }
}
