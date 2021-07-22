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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import org.apache.iceberg.relocated.com.google.common.primitives.UnsignedBytes;
import org.junit.Assert;
import org.junit.Test;

// import java.nio.charset.StandardCharsets;
// import org.apache.commons.lang.RandomStringUtils;

public class TestZOrderByteUtil {
  private static final byte IIIIIIII = (byte) 255;
  private static final byte IOIOIOIO = (byte) 170;
  private static final byte OIOIOIOI = (byte) 85;
  private static final byte OOOOIIII = (byte) 15;
  private static final byte OOOOOOOI = (byte) 1;
  private static final byte OOOOOOOO = (byte) 0;

  private static final int NUM_TESTS = 1000;

  private final Random random = new Random(42);

  @Test
  public void testInterleaveEmptyBits() {
    byte[][] test = new byte[4][10];
    byte[] expected = new byte[40];

    Assert.assertArrayEquals("Should combine empty arrays",
        expected, ZOrderByteUtils.interleaveBits(test));
  }

  @Test
  public void testInterleaveFullBits() {
    byte[][] test = new byte[4][];
    test[0] = new byte[]{IIIIIIII, IIIIIIII};
    test[1] = new byte[]{IIIIIIII};
    test[2] = new byte[0];
    test[3] = new byte[]{IIIIIIII, IIIIIIII, IIIIIIII};
    byte[] expected = new byte[]{IIIIIIII, IIIIIIII, IIIIIIII, IIIIIIII, IIIIIIII, IIIIIIII};

    Assert.assertArrayEquals("Should combine full arrays",
        expected, ZOrderByteUtils.interleaveBits(test));
  }

  @Test
  public void testInterleaveMixedBits() {
    byte[][] test = new byte[4][];
    test[0] = new byte[]{OOOOOOOI, IIIIIIII, OOOOOOOO, OOOOIIII};
    test[1] = new byte[]{OOOOOOOI, OOOOOOOO, IIIIIIII};
    test[2] = new byte[]{OOOOOOOI};
    test[3] = new byte[]{OOOOOOOI};
    byte[] expected = new byte[]{
        OOOOIIII, OOOOOOOO, OOOOOOOO, OOOOOOOO,
        OIOIOIOI, OIOIOIOI,
        IOIOIOIO, IOIOIOIO,
        OOOOIIII};
    Assert.assertArrayEquals("Should combine mixed byte arrays",
        expected, ZOrderByteUtils.interleaveBits(test));
  }

  @Test
  public void testIntOrdering() {
    for (int i = 0; i < NUM_TESTS; i++) {
      int aInt = random.nextInt();
      int bInt = random.nextInt();
      int intCompare = Integer.compare(aInt, bInt);
      byte[] aBytes = ZOrderByteUtils.orderIntLikeBytes(bytesOf(aInt));
      byte[] bBytes = ZOrderByteUtils.orderIntLikeBytes(bytesOf(bInt));
      int byteCompare = UnsignedBytes.lexicographicalComparator().compare(aBytes, bBytes);

      Assert.assertTrue(String.format(
          "Ordering of ints should match ordering of bytes, %s ~ %s -> %s != %s ~ %s -> %s ",
          aInt, bInt, intCompare, Arrays.toString(aBytes), Arrays.toString(bBytes), byteCompare),
          (intCompare ^ byteCompare) >= 0);
    }
  }

  @Test
  public void testLongOrdering() {
    for (int i = 0; i < NUM_TESTS; i++) {
      long aLong = random.nextInt();
      long bLong = random.nextInt();
      int intCompare = Long.compare(aLong, bLong);
      byte[] aBytes = ZOrderByteUtils.orderIntLikeBytes(bytesOf(aLong));
      byte[] bBytes = ZOrderByteUtils.orderIntLikeBytes(bytesOf(bLong));
      int byteCompare = UnsignedBytes.lexicographicalComparator().compare(aBytes, bBytes);

      Assert.assertTrue(String.format(
          "Ordering of ints should match ordering of bytes, %s ~ %s -> %s != %s ~ %s -> %s ",
          aLong, bLong, intCompare, Arrays.toString(aBytes), Arrays.toString(bBytes), byteCompare),
          (intCompare ^ byteCompare) >= 0);
    }
  }

  @Test
  public void testFloatOrdering() {
    for (int i = 0; i < NUM_TESTS; i++) {
      float aFloat = random.nextFloat();
      float bFloat = random.nextFloat();
      int intCompare = Float.compare(aFloat, bFloat);
      byte[] aBytes = ZOrderByteUtils.orderFloatLikeBytes(bytesOf(aFloat));
      byte[] bBytes = ZOrderByteUtils.orderFloatLikeBytes(bytesOf(bFloat));
      int byteCompare = UnsignedBytes.lexicographicalComparator().compare(aBytes, bBytes);

      Assert.assertTrue(String.format(
          "Ordering of ints should match ordering of bytes, %s ~ %s -> %s != %s ~ %s -> %s ",
          aFloat, bFloat, intCompare, Arrays.toString(aBytes), Arrays.toString(bBytes), byteCompare),
          (intCompare ^ byteCompare) >= 0);
    }
  }

  @Test
  public void testDoubleOrdering() {
    for (int i = 0; i < NUM_TESTS; i++) {
      double aDouble = random.nextDouble();
      double bDouble = random.nextDouble();
      int intCompare = Double.compare(aDouble, bDouble);
      byte[] aBytes = ZOrderByteUtils.orderFloatLikeBytes(bytesOf(aDouble));
      byte[] bBytes = ZOrderByteUtils.orderFloatLikeBytes(bytesOf(bDouble));
      int byteCompare = UnsignedBytes.lexicographicalComparator().compare(aBytes, bBytes);

      Assert.assertTrue(String.format(
          "Ordering of ints should match ordering of bytes, %s ~ %s -> %s != %s ~ %s -> %s ",
          aDouble, bDouble, intCompare, Arrays.toString(aBytes), Arrays.toString(bBytes), byteCompare),
          (intCompare ^ byteCompare) >= 0);
    }
  }

//  @Test
//  public void testStringOrdering() {
//    for (int i = 0; i < NUM_TESTS; i++) {
//      String aString = RandomStringUtils.random(random.nextInt(35), true, true);
//      String bString = RandomStringUtils.random(random.nextInt(35), true, true);
//      int intCompare = aString.compareTo(bString);
//      byte[] aBytes = ZOrderByteUtils.orderUTF8LikeBytes(aString.getBytes(StandardCharsets.UTF_8));
//      byte[] bBytes = ZOrderByteUtils.orderUTF8LikeBytes(bString.getBytes(StandardCharsets.UTF_8));
//      int byteCompare = UnsignedBytes.lexicographicalComparator().compare(aBytes, bBytes);
//
//      Assert.assertTrue(String.format(
//          "Ordering of ints should match ordering of bytes, %s ~ %s -> %s != %s ~ %s -> %s ",
//          aString, bString, intCompare, Arrays.toString(aBytes), Arrays.toString(bBytes), byteCompare),
//          (intCompare ^ byteCompare) >= 0);
//    }
//  }

  private byte[] bytesOf(int num) {
    return ByteBuffer.allocate(4).putInt(num).array();
  }

  private byte[] bytesOf(long num) {
    return ByteBuffer.allocate(8).putLong(num).array();
  }

  private byte[] bytesOf(float num) {
    return ByteBuffer.allocate(4).putFloat(num).array();
  }

  private byte[] bytesOf(double num) {
    return ByteBuffer.allocate(8).putDouble(num).array();
  }
}
