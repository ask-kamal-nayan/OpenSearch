/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.exec.text.TextDF;

@ExperimentalApi
public interface DataFormat {
    Setting<Settings> dataFormatSettings();

    Setting<Settings> clusterLeveldataFormatSettings();

    String name();

    void configureStore();

    /**
     * Returns the directory name for this format under the shard path
     * @return the directory name to use for this format
     */
    default String getDirectoryName() {
        return name(); // Default implementation uses format name
    }

    static class LuceneDataFormat implements DataFormat {
        @Override
        public Setting<Settings> dataFormatSettings() {
            return null;
        }

        @Override
        public Setting<Settings> clusterLeveldataFormatSettings() {
            return null;
        }

        @Override
        public String name() {
            return "lucene";
        }

        @Override
        public void configureStore() {

        }

        @Override
        public String getDirectoryName() {
            return "lucene";
        }
    }

    static class ParquetDataFormat implements DataFormat {
        @Override
        public Setting<Settings> dataFormatSettings() {
            return null;
        }

        @Override
        public Setting<Settings> clusterLeveldataFormatSettings() {
            return null;
        }

        @Override
        public String name() {
            return "parquet";
        }

        @Override
        public void configureStore() {

        }

        @Override
        public String getDirectoryName() {
            return "parquet";
        }
    }

    DataFormat LUCENE = new LuceneDataFormat();

    DataFormat PARQUET = new ParquetDataFormat();

    DataFormat TEXT = new TextDF();

    /**
     * Returns the DataFormat instance for the given name.
     * This method provides compatibility with enum-like valueOf behavior.
     * 
     * @param name the format name
     * @return the DataFormat instance
     * @throws IllegalArgumentException if the format name is not recognized
     */
    static DataFormat valueOf(String name) {
        switch (name.toUpperCase()) {
            case "LUCENE":
                return LUCENE;
            case "PARQUET":
                return PARQUET;
            case "TEXT":
                return TEXT;
            default:
                throw new IllegalArgumentException("Unknown DataFormat: " + name);
        }
    }
}
