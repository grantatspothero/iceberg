/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.util;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class ManifestFileUtil {
  private ManifestFileUtil() {}

  // Sentinel indicating not yet decoded, separate from a decoded null
  private static final Object UNSET = new Object();

  private static boolean canContain(
      List<Types.NestedField> fields,
      List<ManifestFile.PartitionFieldSummary> fieldSummaries,
      Object[] lower,
      Object[] upper,
      Comparator<Object>[] comparators,
      StructLike struct) {
    if (struct.size() != fieldSummaries.size()) {
      return false;
    }

    for (int pos = 0; pos < fieldSummaries.size(); pos += 1) {
      ManifestFile.PartitionFieldSummary summary = fieldSummaries.get(pos);
      Object value = struct.get(pos, Object.class);

      if (value == null) {
        if (!summary.containsNull()) {
          return false;
        }
        continue;
      }

      if (NaNUtil.isNaN(value)) {
        if (summary.containsNaN() != null && !summary.containsNaN()) {
          return false;
        }
        continue;
      }

      Type.PrimitiveType primitive = fields.get(pos).type().asPrimitiveType();
      if (primitive instanceof Types.UnknownType) {
        continue;
      }

      Comparator<Object> comparator = comparators[pos];
      if (comparator == null) {
        comparator = Comparators.forType(primitive);
        comparators[pos] = comparator;
      }

      Object lowerBound = lower[pos];
      if (lowerBound == UNSET) {
        lowerBound = Conversions.fromByteBuffer(primitive, summary.lowerBound());
        lower[pos] = lowerBound;
      }
      if (lowerBound == null || comparator.compare(value, lowerBound) < 0) {
        return false;
      }

      Object upperBound = upper[pos];
      if (upperBound == UNSET) {
        upperBound = Conversions.fromByteBuffer(primitive, summary.upperBound());
        upper[pos] = upperBound;
      }
      if (upperBound == null || comparator.compare(value, upperBound) > 0) {
        return false;
      }
    }

    return true;
  }

  public static boolean canContainAny(
      ManifestFile manifest,
      Iterable<Pair<Integer, StructLike>> partitions,
      Map<Integer, PartitionSpec> specsById) {
    List<ManifestFile.PartitionFieldSummary> fieldSummaries = manifest.partitions();
    if (fieldSummaries == null) {
      return true;
    }

    List<Types.NestedField> fields =
        specsById.get(manifest.partitionSpecId()).partitionType().fields();

    int n = fieldSummaries.size();
    Object[] lower = new Object[n];
    Object[] upper = new Object[n];
    Comparator<Object>[] comparators = (Comparator<Object>[]) new Comparator<?>[n];
    Arrays.fill(lower, UNSET);
    Arrays.fill(upper, UNSET);
    Arrays.fill(comparators, null);

    for (Pair<Integer, StructLike> partition : partitions) {
      if (partition.first() == manifest.partitionSpecId()
          && canContain(fields, fieldSummaries, lower, upper, comparators, partition.second())) {
        return true;
      }
    }

    return false;
  }
}
