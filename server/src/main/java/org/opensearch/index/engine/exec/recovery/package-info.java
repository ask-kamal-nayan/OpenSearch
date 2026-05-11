/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Format-agnostic remote-store recovery protocol.
 *
 * <p>Each data format plugin may supply a {@link
 * org.opensearch.index.engine.exec.recovery.FormatRecoveryCoordinator} to participate in the
 * upload/recovery round-trip: capture opaque bytes at upload, validate + reconstruct on
 * recovery. Coordinators are registered via a node-level {@link
 * org.opensearch.index.engine.exec.recovery.FormatRecoveryRegistry} populated from
 * {@code DataFormatPlugin.getRecoveryCoordinator()}.
 *
 * <p>Core recovery orchestrates; formats own their validation. See
 * {@code docs/design/multi-dataformat-recovery-validation.md}.
 */
package org.opensearch.index.engine.exec.recovery;
