/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.recovery;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Node-level registry of {@link FormatRecoveryCoordinator}s keyed by {@link
 * FormatRecoveryCoordinator#formatName()}. Populated at boot from
 * {@code DataFormatPlugin.getRecoveryCoordinator()}.
 */
@ExperimentalApi
public interface FormatRecoveryRegistry {

    Optional<FormatRecoveryCoordinator> get(String formatName);

    Collection<FormatRecoveryCoordinator> all();

    /** No-op registry. Used when DFA is disabled or no plugins register a coordinator. */
    FormatRecoveryRegistry EMPTY = new FormatRecoveryRegistry() {
        @Override
        public Optional<FormatRecoveryCoordinator> get(String formatName) {
            return Optional.empty();
        }

        @Override
        public Collection<FormatRecoveryCoordinator> all() {
            return Collections.emptyList();
        }
    };

    /**
     * Builds a registry from a collection of coordinators. Throws if two coordinators claim
     * the same {@code formatName}. Uses insertion-ordered map so iteration is deterministic.
     */
    static FormatRecoveryRegistry of(Collection<FormatRecoveryCoordinator> coordinators) {
        Objects.requireNonNull(coordinators, "coordinators");
        Map<String, FormatRecoveryCoordinator> map = new LinkedHashMap<>();
        for (FormatRecoveryCoordinator c : coordinators) {
            FormatRecoveryCoordinator prev = map.put(c.formatName(), c);
            if (prev != null) {
                throw new IllegalStateException("duplicate FormatRecoveryCoordinator for format [" + c.formatName() + "]");
            }
        }
        final Map<String, FormatRecoveryCoordinator> finalMap = Collections.unmodifiableMap(map);
        return new FormatRecoveryRegistry() {
            @Override
            public Optional<FormatRecoveryCoordinator> get(String formatName) {
                return Optional.ofNullable(finalMap.get(formatName));
            }

            @Override
            public Collection<FormatRecoveryCoordinator> all() {
                return finalMap.values();
            }
        };
    }
}
