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

package org.apache.spark.sql.connector.iceberg.expressions;

import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.RewritableTransform;
import org.apache.spark.sql.connector.expressions.Transform;
import scala.Product;
import scala.Serializable;
import scala.collection.Iterator;
import scala.collection.Seq;

public final class ZOrderTransform implements RewritableTransform, Product, Serializable {
  private final Seq<NamedReference> columns;
  private final String name;


  @Override
  public Transform withReferences(Seq<NamedReference> newReferences) {
    return null;
  }

  /**
   * Returns the transform function name.
   */
  @Override
  public String name() {
    return this.name;
  }

  /**
   * Returns all field references in the transform arguments.
   */
  @Override
  public NamedReference[] references() {
    return new NamedReference[0];
  }

  /**
   * Returns the arguments passed to the transform function.
   */
  @Override
  public Expression[] arguments() {
    return new Expression[0];
  }

  /**
   * Format the expression as a human readable SQL-like string.
   */
  @Override
  public String describe() {
    return null;
  }

  @Override
  public Object productElement(int n) {
    return null;
  }

  @Override
  public int productArity() {
    return 0;
  }

  @Override
  public Iterator<Object> productIterator() {
    return Product.super.productIterator();
  }

  @Override
  public String productPrefix() {
    return Product.super.productPrefix();
  }

  @Override
  public boolean canEqual(Object that) {
    return false;
  }

  public ZOrderTransform(final Seq<NamedReference> columns) {
    this.columns = columns;
    Product.$init$(this);
    this.name = "zorder";
  }
}
