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

package org.apache.iceberg.avro;

import java.io.IOException;
import java.util.Arrays;
import org.apache.avro.InvalidAvroMagicException;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileReader12;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.io.DatumReader;

import static org.apache.avro.file.DataFileConstants.MAGIC;


/**
 * Need this custom factory because of this avro bug: https://issues.apache.org/jira/browse/AVRO-2944
 * Upgrading iceberg to use a new avro version is painful because things using iceberg (like trino + spark) use older versions of avro
 * Thus, instead port over this `openReader` method from avro 1.10.1 where the bug is fixed
 */
class DataFileReaderFactory {
    // DataFileReader12.MAGIC is package private
    static final byte[] MAGIC_12 = new byte[] { (byte) 'O', (byte) 'b', (byte) 'j', 0 };

    public static <D> FileReader<D> openReader(SeekableInput in, DatumReader<D> reader) throws IOException {
        if (in.length() < MAGIC.length)
            throw new InvalidAvroMagicException("Not an Avro data file");

        // read magic header
        byte[] magic = new byte[MAGIC.length];
        in.seek(0);
        for (int c = 0; c < magic.length; c += in.read(magic, c, magic.length - c)) {
        }
        in.seek(0);

        if (Arrays.equals(MAGIC, magic)) // current format
            return new DataFileReader<>(in, reader);
        if (Arrays.equals(MAGIC_12, magic)) // 1.2 format
            return new DataFileReader12<>(in, reader);

        throw new InvalidAvroMagicException("Not an Avro data file");
    }
}
