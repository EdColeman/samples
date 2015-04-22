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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Track current splits and rows to generate best fit with current splits and
 * bin size.
 */
public class NearestSplits {

  private final static Logger log = LoggerFactory.getLogger(NearestSplits.class);

  private Text lowerSplit = new Text("");
  private Text upperSplit = new Text("");

  private int lowerBoundDistance = 0;

  private final int maxThresholdCount;
  private final ArrayList<RowCounter> buffer;

  private int currThresholdCount = 0;

  /**
   * Tracks rows from last (lower) split until the next split is found.
   *
   * @param error the error rate 0 < error <= 1.
   * @param binSize the number of rows expected in a bin.
   * @param lowerSplit the current, lower split
   * @param upperSplit the next split (null = +inf)
   */
  public NearestSplits(final double error, final int binSize, final Text lowerSplit, final Text upperSplit){

    int calcThreshold = (int)(binSize * error);

    if(calcThreshold < 1){
      maxThresholdCount = 1;
    } else {
      maxThresholdCount = calcThreshold;
    }

    int numNearestBins = (binSize / maxThresholdCount) + 1;

    log.trace("num error bins={}, maxThresholdCount={}", numNearestBins, maxThresholdCount);

    buffer = new ArrayList<RowCounter>(numNearestBins);

    this.lowerSplit = lowerSplit;
    this.upperSplit = upperSplit;

    log.trace("lower = {}, upper = {}", lowerSplit, upperSplit);
  }

  public void setSum(final int value){
    currThresholdCount = value;
  }

  public void clearSum(){
    currThresholdCount = 0;
  }

  public Text getLowerSplit(){
    return lowerSplit;
  }

  public Text getUpperSplit(){
    return upperSplit;
  }

  public void updateLowerSplit(final Text lowerSplit, final int sum){
    this.lowerSplit = lowerSplit;
    currThresholdCount = sum;
    lowerBoundDistance = 0;
    buffer.clear();
  }

  public int getLowerBoundDistance(){
    return lowerBoundDistance;
  }

  public boolean atLowerSplit(final RowCounter rowCounter){

    if(lowerSplit == null){
      return false;
    }

    Text row = rowCounter.getRowId();
    if((row != null) && (lowerSplit.compareTo(row) == 0)){
      return true;
    }

    return false;
  }

  public boolean atNextUpperSplit(final RowCounter rowCounter){

    Text rowId = rowCounter.getRowId();

    if((lowerSplit != null) && (rowId.compareTo(lowerSplit) < 0)) {

      log.trace("Lower not found");

      currThresholdCount += rowCounter.getCount();

      if(currThresholdCount >= maxThresholdCount){
        buffer.add(new RowCounter(rowId,currThresholdCount));
        currThresholdCount = 0;
      }

      return false;
    }

    if((upperSplit == null) || (rowId.compareTo(upperSplit) < 0)){

      int numRows = rowCounter.getCount();

      lowerBoundDistance += numRows;

      currThresholdCount += numRows;

      if(currThresholdCount >= maxThresholdCount){
        buffer.add(new RowCounter(rowId,currThresholdCount));
        currThresholdCount = 0;
      }

      return false;
    }

    return true;
  }

  public void clearCurrentRows(){
    buffer.clear();
  }

  public Collection<RowCounter> getCurrentRows(){
    return buffer;
  }

  @Override public String toString() {
    return "NearestSplits{" +
        "lowerSplit=" + lowerSplit +
        ", upperSplit=" + upperSplit +
        ", lowerBoundDistance=" + lowerBoundDistance +
        ", buffer=" + buffer +
        '}';
  }

  public void clearLowerBoundDistance() {
    lowerBoundDistance = 0;
  }
}
