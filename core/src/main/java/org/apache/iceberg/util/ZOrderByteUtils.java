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

public class ZOrderByteUtils {

  private static final int STRING_KEY_LENGTH = 64;

  private ZOrderByteUtils() {
  }

  /**
   * Signed ints do not have their bytes in magnitude order because of the sign bit,
   * to fix this we flip the sign bit so that all negatives are ordered before positives. This essentially
   * shifts the 0 value so that we don't break our ordering when we cross the new 0 value;
   */
  public static byte[] orderIntLikeBytes(byte[] intBytes) {
    intBytes[0] = (byte) (intBytes[0] ^ 128);
    return intBytes;
  }

  /**
   * IEEE 754 :
   * “If two floating-point numbers in the same format are ordered (say, x < y),
   * they are ordered the same way when their bits are reinterpreted as sign-magnitude integers.”
   * <p>
   * Which means we can treat floats as sign magnitude integers and then convert those into lexicographically
   * comparable bytes
   * Signed magnitude
   */
  public static byte[] orderFloatLikeBytes(byte[] floatBytes) {
    if ((floatBytes[0] & 128) == 0) {
      // The signed magnitude is positive set the first bit (reversing the sign so they order after negatives)
      floatBytes[0] = (byte) (floatBytes[0] | 128);
    } else {
      // The signed magnitude is negative so flip the sign bit so they come before the positives.
      // Then flip all remaining bits so numbers with greater negative magnitude come before those
      // with less magnitude (reverse the order)
      for (int i = 0; i < floatBytes.length; i++) {
        floatBytes[i] = (byte) ~floatBytes[i];
      }
    }
    return floatBytes;
  }

  /**
   * Strings are lexicographically sortable BUT if we use different byte array lengths we will
   * ruin our Z-Ordering (it requires that the columsn contribute the same number of bytes every time).
   * This implementation just uses a {@link #STRING_KEY_LENGTH} byte value for every string. Truncating some strings
   * and filling in 0 for others. In the future we can use the min/max values of our scan range to reassign an
   * ordering based on the strings's relative position in the range.
   */
  public static byte[] orderUTF8LikeBytes(byte[] stringBytes) {
    return Arrays.copyOf(stringBytes, STRING_KEY_LENGTH);
  }

  public static byte[] interleaveBits(byte[][] columnsBinary) {
    int interleavedSize = Arrays.stream(columnsBinary).mapToInt(a -> a.length).sum();
    byte[] interleavedBytes = new byte[interleavedSize];
    int sourceBit = 0;
    int sourceByte = 0;
    int sourceColumn = 0;
    int interleaveByte = 0;
    int interleaveBit = 0;
    while (interleaveByte < interleavedSize) {
      // Take what we have, Get the source Bit of the source Byte, move it to the interleaveBit position
      interleavedBytes[interleaveByte] =
          (byte) (interleavedBytes[interleaveByte] |
              (columnsBinary[sourceColumn][sourceByte] & 1 << sourceBit) >> sourceBit << interleaveBit);

      if (++interleaveBit == 8) {
        // Finished a byte in our interleave byte array start a new byte
        interleaveByte++;
        interleaveBit = 0;
      }

      // Find next column with a byte we can use
      do {
        if (++sourceColumn == columnsBinary.length) {
          sourceColumn = 0;
          if (++sourceBit == 8) {
            sourceBit = 0;
            sourceByte++;
          }
        }
      } while (columnsBinary[sourceColumn].length <= sourceByte && interleaveByte < interleavedSize);
    }
    return interleavedBytes;
  }
}
