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
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;

import static org.junit.Assert.*;

/**
 * Created by etcoleman on 4/20/15.
 */
public class NearestSplitsTest {

  private final static Logger log = LoggerFactory.getLogger(NearestSplitsTest.class);

  @Test public void foundNextSplit1() throws Exception {

    NearestSplits current = new NearestSplits(0.1, 10, new Text("a"), new Text("c"));

    //boolean found = current.atNextUpperSplit(new RowCounter("b", 1));

    assertFalse(current.atNextUpperSplit(new RowCounter(new Text("b"), 1)));
    assertTrue(current.atNextUpperSplit(new RowCounter(new Text("c"), 1)));

    Collection<RowCounter> rows = current.getCurrentRows();

    assertEquals(1, rows.size());
    assertEquals(new Text("b"), rows.iterator().next().getRowId());

    assertEquals(1, current.getLowerBoundDistance());
  }

  @Test public void plusInf() throws Exception {

    NearestSplits current = new NearestSplits(0.01, 1000000, new Text("a"), null);

    assertFalse(current.atNextUpperSplit(new RowCounter(new Text("b"), 1)));
    assertFalse(current.atNextUpperSplit(new RowCounter(new Text("c"), 1)));

    Collection<RowCounter> rows = current.getCurrentRows();

    assertEquals(2, rows.size());

    Iterator<RowCounter> iterator = rows.iterator();

    assertEquals(new Text("b"), iterator.next().getRowId());
    assertEquals(new Text("c"), iterator.next().getRowId());

    assertEquals(2, current.getLowerBoundDistance());
  }

  @Test public void errorBinning(){

    NearestSplits current = new NearestSplits(0.4, 12, new Text("a"), new Text("c"));

    current.atNextUpperSplit(new RowCounter(new Text("b"), 1));
    current.atNextUpperSplit(new RowCounter(new Text("b1"), 1));
    current.atNextUpperSplit(new RowCounter(new Text("b11"), 1));
    current.atNextUpperSplit(new RowCounter(new Text("b111"), 1));
    current.atNextUpperSplit(new RowCounter(new Text("b2"), 1));
    current.atNextUpperSplit(new RowCounter(new Text("b3"), 1));
    current.atNextUpperSplit(new RowCounter(new Text("b4"), 1));
    current.atNextUpperSplit(new RowCounter(new Text("b5"), 1));
    current.atNextUpperSplit(new RowCounter(new Text("b6"), 1));
    current.atNextUpperSplit(new RowCounter(new Text("b7"), 1));
    current.atNextUpperSplit(new RowCounter(new Text("b8"), 1));
    current.atNextUpperSplit(new RowCounter(new Text("b9"), 1));

    Collection<RowCounter> rows = current.getCurrentRows();

    log.info("Rows {}", rows.size());

    for(RowCounter row : rows){
      log.info("{}",row);
    }
  }

  @Test public void errorBinningWithSums(){

    NearestSplits current = new NearestSplits(0.4, 12, new Text("a"), new Text("c"));

    current.atNextUpperSplit(new RowCounter(new Text("b"), 5));
    current.atNextUpperSplit(new RowCounter(new Text("b1"), 2));
    current.atNextUpperSplit(new RowCounter(new Text("b11"), 2));
    current.atNextUpperSplit(new RowCounter(new Text("b111"), 1));

    assertEquals(10, current.getLowerBoundDistance());

    Collection<RowCounter> rows = current.getCurrentRows();

    log.info("Rows {}", rows.size());

    for(RowCounter row : rows){
      log.info("{}",row);
    }
  }

  @Test public void updateLower(){

    NearestSplits current = new NearestSplits(0.4, 12, new Text("a"), new Text("c"));

    current.atNextUpperSplit(new RowCounter(new Text("b"), 5));

    assertEquals(5, current.getLowerBoundDistance());

    current.updateLowerSplit(new Text("b"),1);

    assertEquals(0, current.getLowerBoundDistance());
    assertEquals(0, current.getCurrentRows().size());

    current.atNextUpperSplit(new RowCounter(new Text("b1"), 2));
    current.atNextUpperSplit(new RowCounter(new Text("b11"), 2));
    current.atNextUpperSplit(new RowCounter(new Text("b111"), 1));

    assertEquals(5, current.getLowerBoundDistance());

    Collection<RowCounter> rows = current.getCurrentRows();

    log.info("Rows {}", rows.size());

    for(RowCounter row : rows){
      log.info("{}",row);
    }
  }
}
