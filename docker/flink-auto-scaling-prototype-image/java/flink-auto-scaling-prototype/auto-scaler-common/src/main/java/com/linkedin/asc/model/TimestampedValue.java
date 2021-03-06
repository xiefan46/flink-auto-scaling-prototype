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
 *
 */
package com.linkedin.asc.model;

/**
 * An immutable pair of a value, and its corresponding timestamp.
 *
 * @param <V> the type of the value
 */
public class TimestampedValue<V> {
  private final V value;
  private final long timestamp;

  public TimestampedValue(V v, long timestamp) {
    this.value = v;
    this.timestamp = timestamp;
  }

  public V getValue() {
    return value;
  }

  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || !getClass().equals(o.getClass())) return false;

    TimestampedValue<?> that = (TimestampedValue<?>) o;

    if (timestamp != that.timestamp) return false;
    return value != null ? value.equals(that.value) : (that.value == null);
  }

  @Override
  public int hashCode() {
    int result = value != null ? value.hashCode() : 0;
    result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
    return result;
  }
}

