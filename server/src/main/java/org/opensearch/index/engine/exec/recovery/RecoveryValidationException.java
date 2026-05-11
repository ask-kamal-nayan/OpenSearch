/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.recovery;

import org.opensearch.OpenSearchException;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;

/**
 * Thrown by a {@link FormatRecoveryCoordinator} when on-disk state fails validation
 * during recovery. Fails the shard's recovery attempt; the cluster allocator retries.
 */
@ExperimentalApi
public class RecoveryValidationException extends OpenSearchException {

    public RecoveryValidationException(String formatName, String reason) {
        super("format [" + formatName + "] recovery validation failed: " + reason);
    }

    public RecoveryValidationException(String formatName, String reason, Throwable cause) {
        super("format [" + formatName + "] recovery validation failed: " + reason, cause);
    }

    public static RecoveryValidationException wrap(String formatName, IOException e) {
        return new RecoveryValidationException(formatName, e.getMessage(), e);
    }
}
