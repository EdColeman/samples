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
import java.util.Iterator;
import java.util.TreeSet;

/**
 * Created by etcoleman on 4/19/15.
 */
public class RowCounterTest {

  private final static Logger log = LoggerFactory.getLogger(RowCounterTest.class);

  static ArrayList<Text> currentSplits = new ArrayList<Text>();

  static {

    currentSplits.add(new Text("201503"));
    currentSplits.add(new Text("201506"));
    currentSplits.add(new Text("201509"));
    currentSplits.add(new Text("201512"));
  }

  @Test
  public void simple1(){

    TreeSet<RowCounter> data1 = new TreeSet<RowCounter>();


    TreeSet<RowCounter> calculateSplits = new TreeSet<RowCounter>();

    data1.add(new RowCounter(new Text("201501"), 1));
    data1.add(new RowCounter(new Text("20150101"), 1));
    data1.add(new RowCounter(new Text("201502"), 1));
    data1.add(new RowCounter(new Text("20150201"), 1));
    data1.add(new RowCounter(new Text("201503"), 1));
    data1.add(new RowCounter(new Text("20150301"), 1));
    data1.add(new RowCounter(new Text("201504"), 1));
    data1.add(new RowCounter(new Text("20150401"), 1));
    data1.add(new RowCounter(new Text("201505"), 1));
    data1.add(new RowCounter(new Text("20150501"), 1));
    data1.add(new RowCounter(new Text("201506"), 1));
    data1.add(new RowCounter(new Text("20150601"), 1));
    data1.add(new RowCounter(new Text("201507"), 1));
    data1.add(new RowCounter(new Text("20150701"), 1));
    data1.add(new RowCounter(new Text("201508"), 1));
    data1.add(new RowCounter(new Text("20150801"), 1));
    data1.add(new RowCounter(new Text("201509"), 1));
    data1.add(new RowCounter(new Text("20150901"), 1));
    data1.add(new RowCounter(new Text("201510"), 1));
    data1.add(new RowCounter(new Text("20151001"), 1));
    data1.add(new RowCounter(new Text("201511"), 1));
    data1.add(new RowCounter(new Text("20151101"), 1));
    data1.add(new RowCounter(new Text("201512"), 1));
    data1.add(new RowCounter(new Text("20151201"), 1));
    data1.add(new RowCounter(new Text("20151202"), 1));

    int numRows = data1.size();
    int numBins = 4;


    double pos = 0;

    int binSize = (numRows + 1) / (numBins);

    log.info("rows = {}, bin size = {}", numRows, binSize);

    ArrayList<Text> subset = new ArrayList<Text>((int)(numBins * 1.5));

    Iterator<RowCounter> rows = data1.iterator();

    int sum = 0;

    while(rows.hasNext()){

      RowCounter row = rows.next();

      sum += row.getCount();

      RowCounter current = findCurrentSplit(row.getRowId(), sum);

      if(sum >= binSize){
        calculateSplits.add(new RowCounter(row.getRowId(), sum));
        sum = 0;
      }
    }

//    int j = 0;
//    for (int i = 0; i < numRows && j < numBins; i++) {
//      pos += r;
//      while (pos > 1) {
//        subset.add(((ArrayList<Text>) endRows).get(i));
//        j++;
//        pos -= 1;
//      }
//    }
//

    Iterator<RowCounter> itor = calculateSplits.iterator();

    while(itor.hasNext()){
      log.debug("{}", itor.next());
    }
  }

  int closestSplitIndex = 0;
  Text closestSplit = new Text("");

  private RowCounter findCurrentSplit(Text rowId, int sum) {

    if(closestSplit.compareTo(rowId) >= 0){
      return null;
    }
    int current = closestSplitIndex;

    for(int index = closestSplitIndex; index < currentSplits.size(); index++){

      Text candidate = currentSplits.get(index);

      if(candidate.compareTo(rowId) >= 0){
        log.info("looking {}, found {}", rowId, candidate);
        closestSplit = candidate;
        return null;
      }
    }

    return null;
  }
}
