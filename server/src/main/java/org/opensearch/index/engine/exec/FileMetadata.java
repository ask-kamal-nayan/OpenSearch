/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import java.util.Objects;

/**
 * Metadata for a file with format information.
 * Used as HashMap key in caching and upload tracking.
 * Equality based only on dataFormat and file for context-independent matching.
 *
 * @opensearch.internal
 */
public record FileMetadata(String dataFormat, String file) {
    
    public FileMetadata {
        Objects.requireNonNull(dataFormat, "dataFormat cannot be null");
        Objects.requireNonNull(file, "file cannot be null");
        
        if (dataFormat.isEmpty()) {
            throw new IllegalArgumentException("dataFormat cannot be empty");
        }
        if (file.isEmpty()) {
            throw new IllegalArgumentException("file cannot be empty");
        }
    }
    
    @Override
    public String toString() {
        return dataFormat + ":" + file;
    }
}
