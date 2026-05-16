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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Files;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestManifestFileUtil {
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "id", Types.IntegerType.get()),
          optional(2, "unknown", Types.UnknownType.get()),
          optional(3, "floats", Types.FloatType.get()));

  @TempDir private Path temp;

  @Test
  public void canContainWithUnknownTypeOnly() throws IOException {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("unknown").build();
    PartitionData partition = new PartitionData(spec.partitionType());
    partition.set(0, "someValue");
    ManifestFile manifestFile = writeManifestWithDataFile(spec, partition);

    assertThat(
            ManifestFileUtil.canContainAny(
                manifestFile,
                ImmutableList.of(Pair.of(spec.specId(), partition)),
                ImmutableMap.of(spec.specId(), spec)))
        .isTrue();
  }

  @Test
  public void canContainWithNaNValueOnly() throws IOException {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("floats").build();
    PartitionData partition = new PartitionData(spec.partitionType());
    partition.set(0, Float.NaN);
    ManifestFile manifestFile = writeManifestWithDataFile(spec, partition);

    assertThat(
            ManifestFileUtil.canContainAny(
                manifestFile,
                ImmutableList.of(Pair.of(spec.specId(), partition)),
                ImmutableMap.of(spec.specId(), spec)))
        .isTrue();
  }

  @Test
  public void canContainWithNullValueOnly() throws IOException {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("floats").build();
    PartitionData partition = new PartitionData(spec.partitionType());
    partition.set(0, null);
    ManifestFile manifestFile = writeManifestWithDataFile(spec, partition);

    assertThat(
            ManifestFileUtil.canContainAny(
                manifestFile,
                ImmutableList.of(Pair.of(spec.specId(), partition)),
                ImmutableMap.of(spec.specId(), spec)))
        .isTrue();
  }

  @Test
  public void canContainWithUnknownType() throws IOException {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA).identity("floats").identity("unknown").build();
    PartitionData partition = new PartitionData(spec.partitionType());
    partition.set(0, 1.0f);
    partition.set(1, "someValue");
    ManifestFile manifestFile = writeManifestWithDataFile(spec, partition);

    assertThat(
            ManifestFileUtil.canContainAny(
                manifestFile,
                ImmutableList.of(Pair.of(spec.specId(), partition)),
                ImmutableMap.of(spec.specId(), spec)))
        .isTrue();
  }

  @Test
  public void canContainAnyScalesToTenThousandManifests() throws IOException {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("id").build();
    Map<Integer, PartitionSpec> specsById = ImmutableMap.of(spec.specId(), spec);

    int numRanges = 20;
    int rangeSize = 50;

    // Write 20 distinct manifest templates covering non-overlapping ranges
    List<ManifestFile> templates = Lists.newArrayList();
    for (int m = 0; m < numRanges; m++) {
      templates.add(writeManifestWithPartitionRange(spec, m * rangeSize, (m + 1) * rangeSize - 1));
    }

    // Replicate to 10K manifests by reference — no additional I/O, read phase dominates
    int totalManifests = 10_000;
    List<ManifestFile> manifests = Lists.newArrayList();
    for (int i = 0; i < totalManifests; i++) {
      manifests.add(templates.get(i % numRanges));
    }

    // One midpoint partition per range — every manifest contains exactly one
    List<Pair<Integer, StructLike>> inRangePartitions = Lists.newArrayList();
    for (int m = 0; m < numRanges; m++) {
      PartitionData p = new PartitionData(spec.partitionType());
      p.set(0, m * rangeSize + rangeSize / 2);
      inRangePartitions.add(Pair.of(spec.specId(), p));
    }

    // 100 partitions above all ranges — no manifest contains any
    List<Pair<Integer, StructLike>> outOfRangePartitions = Lists.newArrayList();
    int aboveMax = numRanges * rangeSize;
    for (int v = aboveMax; v < aboveMax + 100; v++) {
      PartitionData p = new PartitionData(spec.partitionType());
      p.set(0, v);
      outOfRangePartitions.add(Pair.of(spec.specId(), p));
    }

    int matched = 0;
    for (ManifestFile manifest : manifests) {
      if (ManifestFileUtil.canContainAny(manifest, inRangePartitions, specsById)) {
        matched++;
      }
    }
    assertThat(matched).isEqualTo(totalManifests);

    int unmatched = 0;
    for (ManifestFile manifest : manifests) {
      if (!ManifestFileUtil.canContainAny(manifest, outOfRangePartitions, specsById)) {
        unmatched++;
      }
    }
    assertThat(unmatched).isEqualTo(totalManifests);
  }

  private ManifestFile writeManifestWithDataFile(PartitionSpec spec, PartitionData partition)
      throws IOException {
    ManifestWriter<DataFile> writer = ManifestFiles.write(spec, Files.localOutput(temp.toFile()));
    try (writer) {
      writer.add(
          DataFiles.builder(spec)
              .withPath("/path/to/data-a.parquet")
              .withFileSizeInBytes(10)
              .withPartition(partition)
              .withRecordCount(10)
              .build());
    }

    return writer.toManifestFile();
  }

  private ManifestFile writeManifestWithPartitionRange(PartitionSpec spec, int partitionStart, int partitionEnd)
      throws IOException {
    ManifestWriter<DataFile> writer =
        ManifestFiles.write(
            spec,
            Files.localOutput(
                temp.resolve("manifest-" + partitionStart + "-" + partitionEnd + ".avro").toFile()));
    try (writer) {
      for (int v = partitionStart; v <= partitionEnd; v++) {
        PartitionData partition = new PartitionData(spec.partitionType());
        partition.set(0, v);
        writer.add(
            DataFiles.builder(spec)
                .withPath("/path/to/data-" + v + ".parquet")
                .withFileSizeInBytes(10)
                .withPartition(partition)
                .withRecordCount(1)
                .build());
      }
    }
    return writer.toManifestFile();
  }
}
