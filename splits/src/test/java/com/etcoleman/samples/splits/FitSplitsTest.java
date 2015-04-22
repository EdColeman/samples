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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * Created by etcoleman on 4/20/15.
 */
public class FitSplitsTest {

  private final static Logger log = LoggerFactory.getLogger(FitSplitsTest.class);

  @Test
  public void testCalculateSplits() throws Exception {

    int numBins = 3;

    ArrayList<Text> currentSplits = new ArrayList<Text>();

    currentSplits.add(new Text("a"));
    currentSplits.add(new Text("b"));
    currentSplits.add(new Text("c"));
    currentSplits.add(new Text("g"));
    currentSplits.add(new Text("z"));

    TreeSet<RowCounter> data1 = new TreeSet<RowCounter>();
    data1.add(new RowCounter(new Text("a"), 1));
    data1.add(new RowCounter(new Text("b"), 1));
    data1.add(new RowCounter(new Text("c"), 1));
    data1.add(new RowCounter(new Text("d"), 1));

    data1.add(new RowCounter(new Text("e"), 1));
    data1.add(new RowCounter(new Text("f"), 1));
    data1.add(new RowCounter(new Text("g"), 1));
    data1.add(new RowCounter(new Text("h"), 1));

    data1.add(new RowCounter(new Text("i"), 1));
    data1.add(new RowCounter(new Text("j"), 1));

    int numRows = data1.size();

    FitSplits processData = new FitSplits(numBins, numRows, 0.1, currentSplits);

    Iterator<RowCounter> rowData = data1.iterator();

    int sum = 0;

    int binSize = (numRows + 1) / (numBins);

    log.info("bin size {}", binSize);

    while (rowData.hasNext()) {

      RowCounter row = rowData.next();

      sum += row.getCount();

      log.debug("{}", row);

      processData.processRow(row);

    }

    processData.closeProcessing();

    printResults(processData.getCalculatedSplits());

  }

  @Test
  public void testForceSplits() throws Exception {

    int numBins = 4;

    ArrayList<Text> currentSplits = new ArrayList<Text>();

    currentSplits.add(new Text("a"));
    currentSplits.add(new Text("z"));

    TreeSet<RowCounter> data1 = new TreeSet<RowCounter>();
    data1.add(new RowCounter(new Text("a"), 1));
    data1.add(new RowCounter(new Text("b"), 2));
    data1.add(new RowCounter(new Text("c"), 2));

    data1.add(new RowCounter(new Text("d"), 1));
    data1.add(new RowCounter(new Text("e"), 2));
    data1.add(new RowCounter(new Text("f"), 2));

    data1.add(new RowCounter(new Text("g"), 2));
    data1.add(new RowCounter(new Text("h"), 2));
    data1.add(new RowCounter(new Text("i"), 2));

    data1.add(new RowCounter(new Text("j"), 2));

    int numRows = data1.size() * 2;

    FitSplits processData = new FitSplits(numBins, numRows, 0.1, currentSplits);

    Iterator<RowCounter> rowData = data1.iterator();

    int sum = 0;

    int binSize = (numRows + 1) / (numBins);

    log.info("bin size {}", binSize);

    while (rowData.hasNext()) {

      RowCounter row = rowData.next();

      sum += row.getCount();

      log.debug("{}", row);

      processData.processRow(row);


    }

    processData.closeProcessing();

    printResults(processData.getCalculatedSplits());

  }

  private void printResults(Collection<RowCounter> calculatedSplits) {
    for(RowCounter split : calculatedSplits){
      log.info("{}", split);
    }
  }

  @Test
  public void testUsedClosePastSplits() throws Exception {

    int numBins = 2;

    ArrayList<Text> currentSplits = new ArrayList<Text>();

    currentSplits.add(new Text("a"));
    currentSplits.add(new Text("e"));
    currentSplits.add(new Text("z"));

    TreeSet<RowCounter> data1 = new TreeSet<RowCounter>();
    data1.add(new RowCounter(new Text("a"), 1));
    data1.add(new RowCounter(new Text("b"), 2));
    data1.add(new RowCounter(new Text("c"), 2));
    data1.add(new RowCounter(new Text("d"), 1));
    data1.add(new RowCounter(new Text("e"), 2));

    data1.add(new RowCounter(new Text("f"), 2));
    data1.add(new RowCounter(new Text("g"), 2));
    data1.add(new RowCounter(new Text("h"), 2));
    data1.add(new RowCounter(new Text("i"), 2));

    data1.add(new RowCounter(new Text("j"), 2));

    int numRows = data1.size() * 2;

    FitSplits processData = new FitSplits(numBins, numRows, 0.2, currentSplits);

    Iterator<RowCounter> rowData = data1.iterator();

    int sum = 0;

    int binSize = (numRows + 1) / (numBins);

    log.info("bin size {}", binSize);

    while (rowData.hasNext()) {

      RowCounter row = rowData.next();

      sum += row.getCount();

      log.debug("{}", row);

      processData.processRow(row);


    }

    processData.closeProcessing();;

    printResults(processData.getCalculatedSplits());

  }

  @Test
  public void testBigSplits() throws Exception {

    int numBins = 3;

    ArrayList<Text> currentSplits = new ArrayList<Text>();

    currentSplits.add(new Text("a"));
    currentSplits.add(new Text("f"));
    currentSplits.add(new Text("z"));

    TreeSet<RowCounter> data1 = new TreeSet<RowCounter>();
    data1.add(new RowCounter(new Text("a"), 1));
    data1.add(new RowCounter(new Text("b"), 20));
    data1.add(new RowCounter(new Text("c"), 20));

    data1.add(new RowCounter(new Text("d"), 1));
    data1.add(new RowCounter(new Text("e"), 2));
    data1.add(new RowCounter(new Text("f"), 2));

    data1.add(new RowCounter(new Text("g"), 2));
    data1.add(new RowCounter(new Text("h"), 2));
    data1.add(new RowCounter(new Text("i"), 2));

    data1.add(new RowCounter(new Text("j"), 2));

    int numRows = data1.size() * 2;

    FitSplits processData = new FitSplits(numBins, numRows, 0.1, currentSplits);

    Iterator<RowCounter> rowData = data1.iterator();

    int sum = 0;

    int binSize = (numRows + 1) / (numBins);

    log.info("bin size {}", binSize);

    while (rowData.hasNext()) {

      RowCounter row = rowData.next();

      sum += row.getCount();

      log.debug("{}", row);

      processData.processRow(row);


    }

    processData.closeProcessing();;

    printResults(processData.getCalculatedSplits());

  }

}
