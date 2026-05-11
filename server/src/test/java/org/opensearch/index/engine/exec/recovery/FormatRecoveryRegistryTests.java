/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.recovery;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class FormatRecoveryRegistryTests extends OpenSearchTestCase {

    public void testEmptyRegistry() {
        assertTrue(FormatRecoveryRegistry.EMPTY.all().isEmpty());
        assertEquals(Optional.empty(), FormatRecoveryRegistry.EMPTY.get("lucene"));
        assertEquals(Optional.empty(), FormatRecoveryRegistry.EMPTY.get("parquet"));
    }

    public void testOfSingleCoordinator() {
        FormatRecoveryCoordinator lucene = new NamedCoordinator("lucene");
        FormatRecoveryRegistry registry = FormatRecoveryRegistry.of(List.of(lucene));

        assertEquals(1, registry.all().size());
        assertSame(lucene, registry.get("lucene").orElseThrow());
        assertEquals(Optional.empty(), registry.get("parquet"));
    }

    public void testOfMultipleCoordinators() {
        FormatRecoveryCoordinator lucene = new NamedCoordinator("lucene");
        FormatRecoveryCoordinator parquet = new NamedCoordinator("parquet");
        FormatRecoveryRegistry registry = FormatRecoveryRegistry.of(List.of(lucene, parquet));

        assertEquals(2, registry.all().size());
        assertSame(lucene, registry.get("lucene").orElseThrow());
        assertSame(parquet, registry.get("parquet").orElseThrow());
    }

    public void testOfRejectsDuplicateFormatName() {
        FormatRecoveryCoordinator first = new NamedCoordinator("lucene");
        FormatRecoveryCoordinator second = new NamedCoordinator("lucene");
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> FormatRecoveryRegistry.of(List.of(first, second)));
        assertTrue(e.getMessage().contains("duplicate"));
        assertTrue(e.getMessage().contains("lucene"));
    }

    public void testOfEmptyCollection() {
        FormatRecoveryRegistry registry = FormatRecoveryRegistry.of(List.of());
        assertTrue(registry.all().isEmpty());
        assertEquals(Optional.empty(), registry.get("anything"));
    }

    private static final class NamedCoordinator implements FormatRecoveryCoordinator {
        private final String name;

        NamedCoordinator(String name) {
            this.name = name;
        }

        @Override
        public String formatName() {
            return name;
        }

        @Override
        public byte[] captureForUpload(UploadContext context) throws IOException {
            return null;
        }

        @Override
        public void onRecovery(byte[] state, RecoveryContext context) throws IOException {}
    }
}
