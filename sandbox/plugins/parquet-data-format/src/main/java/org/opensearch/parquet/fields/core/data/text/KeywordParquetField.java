/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields.core.data.text;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.vsr.ManagedVSR;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Parquet field for keyword values using {@link VarCharVector} with UTF-8 encoding.
 */
public class KeywordParquetField extends ParquetField {

    /** Creates a new KeywordParquetField. */
    public KeywordParquetField() {}

    @Override
    protected void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        addToVector(managedVSR.getVector(mappedFieldType.name()), managedVSR.getRowCount(), parseValue);
    }

    @Override
    protected void addToVector(FieldVector vector, int index, Object parseValue) {
        ((VarCharVector) vector).setSafe(index, parseValue.toString().getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Keyword values are stored as UTF-8 bytes and the read path ({@code ParquetSortedSetDocValues})
     * sorts them as {@link org.apache.lucene.util.BytesRef}, i.e. by unsigned byte order.
     * {@link String#compareTo} uses UTF-16 code-unit order, which diverges for non-ASCII and
     * supplementary characters, so we compare the encoded UTF-8 bytes here to reproduce the
     * reader's ordering exactly — otherwise a file marked values-sorted could serve keyword
     * multi-values in the wrong order.
     */
    @Override
    protected Comparator<Object> listElementComparator() {
        return Comparator.comparing(v -> v.toString().getBytes(StandardCharsets.UTF_8), Arrays::compareUnsigned);
    }

    @Override
    public boolean supportsMultiValue() {
        return true;
    }

    @Override
    public ArrowType getArrowType() {
        return new ArrowType.Utf8();
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.nullable(getArrowType());
    }
}
