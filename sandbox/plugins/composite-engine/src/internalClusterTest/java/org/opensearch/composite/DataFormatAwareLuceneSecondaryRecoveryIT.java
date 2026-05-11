/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Minimal reproducer for the "catalog references .si file missing on disk" recovery bug.
 * Uses {@code primary_data_format=lucene} with {@code secondary_data_formats=[parquet]} so the
 * composite catalog contains both Lucene {@code .si} files and parquet files. Restarts the
 * primary node to trigger {@code existing store recovery} (CLUSTER_RECOVERED), which exercises
 * the recovery path where {@code updateReplicationCheckpoint} computes metadata over all
 * catalog files.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DataFormatAwareLuceneSecondaryRecoveryIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "dfa-lucene-secondary-recovery-test";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(
            super.nodePlugins().stream(),
            Stream.of(ParquetDataFormatPlugin.class, CompositeDataFormatPlugin.class, LucenePlugin.class, DataFusionPlugin.class)
        ).collect(Collectors.toList());
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .build();
    }

    /** Primary=parquet, Secondary=lucene. Catalog tracks BOTH formats; .si files are real files on disk. */
    private Settings luceneSecondaryParquetSettings() {
        return Settings.builder()
            .put(remoteStoreIndexSettings(0, 1))
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", List.of("lucene"))
            .build();
    }

    /** Matches user's clickbench-6 settings exactly: refresh_interval=-1, no caches. */
    private Settings clickbenchLikeSettings() {
        return Settings.builder()
            .put(remoteStoreIndexSettings(0, 1))
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put("index.refresh_interval", "-1")
            .put("index.queries.cache.enabled", false)
            .put("index.requests.cache.enable", false)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", List.of("lucene"))
            .build();
    }

    private void indexDocs(int count) {
        for (int i = 0; i < count; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource("field_text", randomAlphaOfLength(10), "field_keyword", randomAlphaOfLength(10), "field_number", (long) i)
                .get();
        }
    }

    private String primaryNodeName() {
        String nodeId = getClusterState().routingTable().index(INDEX_NAME).shard(0).primaryShard().currentNodeId();
        for (String nodeName : internalCluster().getNodeNames()) {
            if (internalCluster().clusterService(nodeName).localNode().getId().equals(nodeId)) {
                return nodeName;
            }
        }
        throw new IllegalStateException("node not found for id " + nodeId);
    }

    /**
     * Reproducer: index docs with composite (lucene primary + parquet secondary), flush, restart
     * the primary node. On recovery, the engine opens and {@code updateReplicationCheckpoint}
     * walks every file in the catalog — including Lucene {@code .si} files — and must find them
     * on disk.
     */
    public void testPrimaryRestartRecoveryWithLucenePrimaryAndParquetSecondary() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(luceneSecondaryParquetSettings()).get();
        ensureGreen(INDEX_NAME);

        indexDocs(randomIntBetween(20, 50));
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Capture the pre-restart state: which Lucene files exist on disk?
        IndexShard preShard = getIndexShard(primaryNodeName(), INDEX_NAME);
        java.nio.file.Path indexDir = preShard.shardPath().resolveIndex();
        java.util.Set<String> preLuceneFiles = listLuceneFiles(indexDir);
        assertFalse("precondition: must have at least one Lucene segment file (.si/.cfe/.cfs)", preLuceneFiles.isEmpty());
        logger.info("Pre-restart Lucene files on disk: {}", preLuceneFiles);

        // Sanity: the primary has a catalog pointing at .si files.
        assertBusy(() -> {
            IndexShard shard = getIndexShard(primaryNodeName(), INDEX_NAME);
            assertNotNull(shard);
            DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(shard);
        }, 30, TimeUnit.SECONDS);

        // Restart the primary — triggers existing-store recovery (CLUSTER_RECOVERED) on that node.
        String nodeName = primaryNodeName();
        internalCluster().restartNode(nodeName);
        ensureGreen(INDEX_NAME);

        // After restart the primary must be healthy and the catalog/local/remote tiers must agree.
        // Critical validation: the Lucene .si/.cfe/.cfs files that existed before must still exist
        // — proving that IndexWriter on engine open did NOT delete them as unreferenced.
        assertBusy(() -> {
            IndexShard recovered = getIndexShard(primaryNodeName(), INDEX_NAME);
            assertNotNull("recovered shard must not be null", recovered);
            assertTrue("shard must be started after restart", recovered.routingEntry().started());

            java.nio.file.Path recoveredIndexDir = recovered.shardPath().resolveIndex();
            java.util.Set<String> postLuceneFiles = listLuceneFiles(recoveredIndexDir);
            logger.info("Post-restart Lucene files on disk: {}", postLuceneFiles);
            assertTrue(
                "Lucene files must survive recovery. pre=" + preLuceneFiles + " post=" + postLuceneFiles,
                postLuceneFiles.containsAll(preLuceneFiles)
            );

            DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(recovered);
        }, 60, TimeUnit.SECONDS);
    }

    /**
     * Lists Lucene segment files (.si, .cfe, .cfs, .nvd, .fdt, etc.) in the given directory,
     * excluding {@code segments_N} / {@code write.lock} / pending_segments.
     */
    private java.util.Set<String> listLuceneFiles(java.nio.file.Path indexDir) throws java.io.IOException {
        if (!java.nio.file.Files.isDirectory(indexDir)) {
            return java.util.Collections.emptySet();
        }
        try (var stream = java.nio.file.Files.list(indexDir)) {
            return stream.map(p -> p.getFileName().toString())
                .filter(n -> n.contains("_"))
                .filter(n -> !n.startsWith("segments"))
                .filter(n -> !n.equals("write.lock"))
                .collect(java.util.stream.Collectors.toSet());
        }
    }

    /**
     * Matches the user's clickbench-6 reproducer exactly:
     * <ul>
     *   <li>3 data nodes (like {@code ./gradlew run -PnumNodes=3})</li>
     *   <li>{@code refresh_interval=-1}, caches disabled</li>
     *   <li>primary=parquet, secondary=lucene</li>
     *   <li>Manual refresh ONLY — NO flush (matches Ctrl+C without clean shutdown)</li>
     * </ul>
     * Before the fix in LuceneIndexingExecutionEngine.refresh (sharedWriter.commit after addIndexes),
     * this scenario leaves the local Lucene files as orphans — IndexWriter holds them as pending
     * segments that are never committed to segments_N. A full cluster restart loses the in-memory
     * pending state and the engine then deletes the "unreferenced" files on open.
     */
    public void testFullClusterRestartMatchesClickbenchScenario() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(clickbenchLikeSettings()).get();
        ensureGreen(INDEX_NAME);

        indexDocs(randomIntBetween(20, 50));
        // NO FLUSH — only refresh. This is the exact user scenario that was previously broken:
        // refresh triggers IndexWriter.addIndexes but not IndexWriter.commit, so segments_N on
        // disk never references the Lucene files. Ctrl+C then leaves orphan files.
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Capture pre-restart Lucene file state on the primary node.
        IndexShard preShard = getIndexShard(primaryNodeName(), INDEX_NAME);
        java.nio.file.Path indexDir = preShard.shardPath().resolveIndex();
        java.util.Set<String> preLuceneFiles = listLuceneFiles(indexDir);
        logger.info("Pre-restart Lucene files: {}", preLuceneFiles);
        assertFalse("precondition: must have at least one Lucene file on disk", preLuceneFiles.isEmpty());

        // FULL CLUSTER RESTART — matches user's Ctrl+C + restart scenario.
        internalCluster().fullRestart();
        ensureGreen(INDEX_NAME);

        // Validate recovery completed cleanly: shard started + files preserved + catalog consistent.
        assertBusy(() -> {
            IndexShard recovered = getIndexShard(primaryNodeName(), INDEX_NAME);
            assertNotNull("shard must not be null after full cluster restart", recovered);
            assertTrue("shard must be started after full cluster restart", recovered.routingEntry().started());

            java.nio.file.Path postIndexDir = recovered.shardPath().resolveIndex();
            java.util.Set<String> postLuceneFiles = listLuceneFiles(postIndexDir);
            logger.info("Post-restart Lucene files: {}", postLuceneFiles);
            assertTrue(
                "Lucene files must survive full cluster restart. pre=" + preLuceneFiles + " post=" + postLuceneFiles,
                postLuceneFiles.containsAll(preLuceneFiles)
            );

            DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(recovered);
        }, 120, TimeUnit.SECONDS);
    }
}
