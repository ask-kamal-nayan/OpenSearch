/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.util.concurrent.AbstractRefCounted;
import org.opensearch.index.engine.exec.RefreshResult;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.FileMetadata;

import java.io.Serializable;
import java.util.*;

@ExperimentalApi
public class CatalogSnapshot extends AbstractRefCounted {

    private final long id;
    private long version;
    private final Map<String, Collection<WriterFileSet>> dfGroupedSearchableFiles;
    private Map<String, String> userData;

    public CatalogSnapshot(RefreshResult refreshResult, long id) {
        super("catalog_snapshot");
        this.id = id;
        this.version = 0;
        this.userData = new HashMap<>();
        this.dfGroupedSearchableFiles = new HashMap<>();
        if (refreshResult != null) {
            refreshResult.getRefreshedFiles().forEach((dataFormat, writerFiles) -> dfGroupedSearchableFiles.put(dataFormat.name(), writerFiles));
        }
    }

    public Collection<WriterFileSet> getSearchableFiles(String dataFormat) {
        if (dfGroupedSearchableFiles.containsKey(dataFormat)) {
            return dfGroupedSearchableFiles.get(dataFormat);
        }
        return Collections.emptyList();
    }

    public Collection<Segment> getSegments() {
        Map<Long, Segment> segmentMap = new HashMap<>();
        dfGroupedSearchableFiles.forEach((dataFormat, writerFileSets) -> writerFileSets.forEach(writerFileSet -> {
            Segment segment = segmentMap.computeIfAbsent(writerFileSet.getWriterGeneration(), Segment::new);
            segment.addSearchableFiles(dataFormat, writerFileSet);
        }));
        return Collections.unmodifiableCollection(segmentMap.values());
    }

    public Collection<FileMetadata> getFileMetadataList() {
        Collection<Segment> segments = getSegments();
        Collection<FileMetadata> allFileMetadata = new ArrayList<>();

        for (Segment segment : segments) {
            // Each segment contains multiple data formats
            segment.dfGroupedSearchableFiles.forEach((dataFormatName, writerFileSet) -> {
                // Create FileMetadata for each file in this WriterFileSet
                for (String fileName : writerFileSet.getFiles()) {
                    FileMetadata fileMetadata = new FileMetadata(
                        dataFormatName,  // String dataFormat
                        fileName        // String file
                    );
                    allFileMetadata.add(fileMetadata);
                }
            });
        }

        return allFileMetadata;
    }

    public long getGeneration() {
        return id;
    }

    public long getVersion() {
        return version;
    }

    /**
     * Returns user data associated with this catalog snapshot.
     * TODO: Implement proper user data storage when catalog snapshot supports it
     *
     * @return map of user data key-value pairs
     */
    public Map<String, String> getUserData() {
        return new HashMap<>(userData);
    }

    /**
     * Sets user data for this catalog snapshot.
     * TODO: Implement proper user data storage when catalog snapshot supports it
     *
     * @param userData the user data map to set
     * @param doIncrementVersion whether to increment version (for compatibility with SegmentInfos API)
     */
    public void setUserData(Map<String, String> userData, boolean doIncrementVersion) {
        if (userData == null) {
            this.userData = Collections.emptyMap();
        } else {
            this.userData = new HashMap<>(userData);
        }
        if (doIncrementVersion) {
            changed();
        }
    }

    private void changed() {
        version++;
    }

    @Override
    protected void closeInternal() {
        // notify to file deleter, search, etc
    }

    public long getId() {
        return id;
    }

    /**
     * Creates a clone of this CatalogSnapshot with the same content but independent lifecycle.
     * Similar to SegmentInfos.clone(), this allows creating independent copies that can be
     * modified without affecting the original snapshot.
     *
     * @return a new CatalogSnapshot instance with the same data
     */
    public CatalogSnapshot clone() {
        // Create new CatalogSnapshot with same ID and deep copy of data
        CatalogSnapshot cloned = new CatalogSnapshot(null, this.id);
        cloned.version = this.version;
        cloned.userData = new HashMap<>(this.userData);

        // Deep copy the dfGroupedSearchableFiles map
        for (Map.Entry<String, Collection<WriterFileSet>> entry : this.dfGroupedSearchableFiles.entrySet()) {
            String dataFormat = entry.getKey();
            Collection<WriterFileSet> writerFileSets = entry.getValue();

            // Create a new collection with the same WriterFileSet references
            // WriterFileSet objects are typically immutable, so shallow copy of the collection is sufficient
            Collection<WriterFileSet> clonedWriterFileSets = new ArrayList<>(writerFileSets);
            cloned.dfGroupedSearchableFiles.put(dataFormat, clonedWriterFileSets);
        }

        return cloned;
    }

    @Override
    public String toString() {
        return "CatalogSnapshot{" +
            "id=" + id +
            ", version=" + version +
            ", dfGroupedSearchableFiles=" + dfGroupedSearchableFiles +
            '}';
    }

    public static class Segment implements Serializable {

        private final long generation;
        private final Map<String, WriterFileSet> dfGroupedSearchableFiles;

        public Segment(long generation) {
            this.dfGroupedSearchableFiles = new HashMap<>();
            this.generation = generation;
        }

        public void addSearchableFiles(String dataFormat, WriterFileSet writerFileSetGroup) {
            dfGroupedSearchableFiles.put(dataFormat, writerFileSetGroup);
        }

        public long getGeneration() {
            return generation;
        }

        @Override
        public String toString() {
            return "Segment{" + "generation=" + generation + ", dfGroupedSearchableFiles=" + dfGroupedSearchableFiles + '}';
        }
    }
}
