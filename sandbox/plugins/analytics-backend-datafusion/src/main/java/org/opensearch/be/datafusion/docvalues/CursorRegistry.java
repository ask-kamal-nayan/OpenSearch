/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.opensearch.be.datafusion.docvalues.bridge.ParquetColumnReader;
import org.opensearch.be.datafusion.docvalues.bridge.ParquetListColumnReader;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Per-request registry of the dedicated cursors one request opened through its
 * {@link ParquetDocValuesLeafReader}.
 *
 * <p>The doc-values accessor API ({@code getSortedNumericDocValues(field)}) carries no request
 * identity, and under concurrent segment search the slice threads serving one request do not map
 * back to it, so the request-scoped wrapper is the only place that knows which cursors to close when
 * the request ends. The segment-lifetime producer is shared across requests and is not closed here.
 */
final class CursorRegistry implements Closeable {

    private final List<ParquetColumnReader> cursors = Collections.synchronizedList(new ArrayList<>());
    // Multi-valued list cursors registered on the same request lifecycle. Kept in a list separate
    // from the scalar cursors so opened() still exposes only the scalar view callers/tests expect,
    // while close() releases both. The scalar `cursors` list is the single monitor guarding `closed`
    // for both registration paths, so a list-cursor open racing a concurrent close is handled exactly
    // like a scalar one and never leaks a native handle.
    private final List<ParquetListColumnReader> listCursors = Collections.synchronizedList(new ArrayList<>());
    private boolean closed;

    /** Records a scalar cursor this request opened; it is closed by {@link #close()} at request end. */
    void register(ParquetColumnReader cursor) {
        synchronized (cursors) {
            if (closed) {
                // The request already ended; close immediately rather than record on a drained list.
                cursor.close();
                throw new IllegalStateException("cursor registry is closed");
            }
            cursors.add(cursor);
        }
    }

    /**
     * Records a multi-valued list cursor this request opened, released by {@link #close()} on the same
     * lifecycle as scalar cursors so a request's list cursors never outlive it.
     */
    void register(ParquetListColumnReader cursor) {
        synchronized (cursors) {
            if (closed) {
                // The request already ended; close immediately rather than record on a drained list.
                cursor.close();
                throw new IllegalStateException("cursor registry is closed");
            }
            listCursors.add(cursor);
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (cursors) {
            if (closed) {
                return;
            }
            closed = true;
            for (ParquetColumnReader cursor : cursors) {
                // Idempotent through NativeHandle; close never throws checked exceptions.
                cursor.close();
            }
            for (ParquetListColumnReader cursor : listCursors) {
                // Same idempotent NativeHandle teardown as scalar cursors, on the same request lifecycle.
                cursor.close();
            }
            cursors.clear();
            listCursors.clear();
        }
    }

    /** Scalar cursors opened on this request (tests). */
    List<ParquetColumnReader> opened() {
        synchronized (cursors) {
            return List.copyOf(cursors);
        }
    }
}
