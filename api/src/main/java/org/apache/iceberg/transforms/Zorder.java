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

package org.apache.iceberg.transforms;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.davidmoten.hilbert.HilbertCurve;


class Zorder implements Transform<List<Object>, Integer> {
  public static Zorder get() {
    return new Zorder();
  }

  private HilbertCurve curve = null;

  private Zorder(){

  }

  /**
   * Transforms a value to its corresponding partition value.
   *
   * @param value a source value
   * @return a transformed partition value
   */
  @Override
  public Integer apply(List<Object> value) {
    if (curve == null) {
      curve = HilbertCurve.bits(Integer.SIZE - 1).dimensions(value.size());
    }

    List<Long> valueList = new ArrayList<>();
    value.stream().filter(v -> v instanceof Integer).forEach(v -> valueList.add(Long.valueOf(v.toString())));
    long[] pointArray = valueList.stream().mapToLong(i -> i).toArray();
    BigInteger ret = curve.index(pointArray);
    Integer finalret = ret.intValue();
    return finalret;
  }

  /**
   * Checks whether this function can be applied to the given {@link Type}.
   *
   * @param type a type
   * @return true if this transform can be applied to the type, false otherwise
   */
  @Override
  public boolean canTransform(Type type) {
    return type.typeId() == Type.TypeID.INTEGER;
  }

  /**
   * Returns the {@link Type} produced by this transform given a source type.
   *
   * @param sourceType a type
   * @return the result type created by the apply method for the given type
   */
  @Override
  public Type getResultType(Type sourceType) {
    return Types.IntegerType.get();
  }

  /**
   * Whether the transform preserves the order of values (is monotonic).
   * <p>
   * A transform preserves order for values when for any given a and b, if a &lt; b then apply(a) &lt;= apply(b).
   *
   * @return true if the transform preserves the order of values
   */
  @Override
  public boolean preservesOrder() {
    return true;
  }

  /**
   * Whether ordering by this transform's result satisfies the ordering of another transform's result.
   * <p>
   * For example, sorting by day(ts) will produce an ordering that is also by month(ts) or year(ts). However, sorting
   * by day(ts) will not satisfy the order of hour(ts) or identity(ts).
   *
   * @param other Transform for comparison
   * @return true if ordering by this transform is equivalent to ordering by the other transform
   */
  @Override
  public boolean satisfiesOrderOf(Transform<?, ?> other) {
    return Transform.super.satisfiesOrderOf(other);
  }

  @Override
  public UnboundPredicate<Integer> project(String name, BoundPredicate<List<Object>> predicate) {
    return null;
  }

  @Override
  public UnboundPredicate<Integer> projectStrict(String name, BoundPredicate<List<Object>> predicate) {
    return null;
  }

  /**
   * Return whether this transform is the identity transform.
   *
   * @return true if this is an identity transform, false otherwise
   */
  @Override
  public boolean isIdentity() {
    return false;
  }

  @Override
  public String toString() {
    return "zorder";
  }

  /**
   * Returns a human-readable String representation of a transformed value.
   * <p>
   * null values will return "null"
   *
   * @param value a transformed value
   * @return a human-readable String representation of the value
   */
  @Override
  public String toHumanString(Integer value) {
    return Transform.super.toHumanString(value);
  }

  /**
   * Return the unique transform name to check if similar transforms for the same source field
   * are added multiple times in partition spec builder.
   *
   * @return a name used for dedup
   */
  @Override
  public String dedupName() {
    return Transform.super.dedupName();
  }
}
