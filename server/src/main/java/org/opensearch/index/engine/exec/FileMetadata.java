/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import reactor.util.annotation.NonNull;

import java.util.Objects;

public class FileMetadata {

    public static final String DELIMITER = ":::";
    private static final String METADATA_KEY = "metadata";

    private final String file;
    private final String dataFormat;

    public FileMetadata(String dataFormat, String file) {
        this.file = file;
        this.dataFormat = dataFormat;
    }

    public FileMetadata(String dataFormatAwareFile) {
        if (!dataFormatAwareFile.contains(DELIMITER) && dataFormatAwareFile.startsWith(METADATA_KEY)) {
            this.dataFormat = "metadata";
            this.file = dataFormatAwareFile;
            return;
        }
        if (dataFormatAwareFile.contains(DELIMITER)) {
            // Serialized form: "_0.parquet:::parquet"
            String[] parts = dataFormatAwareFile.split(DELIMITER);
            this.dataFormat = parts[1];
            this.file = parts[0];
        } else if (dataFormatAwareFile.contains("/")) {
            // Path-style form: "parquet/_0.parquet"
            int slash = dataFormatAwareFile.indexOf('/');
            this.dataFormat = dataFormatAwareFile.substring(0, slash);
            this.file = dataFormatAwareFile.substring(slash + 1);
        } else {
            // Plain filename: "_0.cfs"
            this.dataFormat = "lucene";
            this.file = dataFormatAwareFile;
        }
    }

    public String serialize() {
        return file + DELIMITER + dataFormat;
    }

    @Override
    public @NonNull String toString() {
        return serialize();
    }

    public String file() {
        return file;
    }

    public String dataFormat() {
        return dataFormat;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        FileMetadata that = (FileMetadata) o;
        return Objects.equals(file, that.file) && Objects.equals(dataFormat, that.dataFormat);
    }

    @Override
    public int hashCode() {
        return Objects.hash(file, dataFormat);
    }
}
