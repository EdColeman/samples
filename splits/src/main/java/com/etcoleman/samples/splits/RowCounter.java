/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.etcoleman.samples.splits;

import org.apache.hadoop.io.Text;

/**
 * Created by etcoleman on 4/19/15.
 */
public class RowCounter implements Comparable<RowCounter> {

  private final int count;
  private final Text rowId;

  public RowCounter(final Text rowId, final int count){
    this.rowId = rowId;
    this.count = count;
  }

  public Text getRowId(){
    return rowId;
  }

  public int getCount(){
    return count;
  }

  public int compareTo(RowCounter other) {
    return this.rowId.compareTo(other.rowId);
  }

  @Override public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    RowCounter that = (RowCounter) o;

    return rowId.equals(that.rowId);

  }

  @Override public int hashCode() {
    return rowId.hashCode();
  }

  @Override public String toString() {
    return "RowCounter{" +
        "rowId='" + rowId + '\'' +
        ", count=" + count +
        '}';
  }
}
