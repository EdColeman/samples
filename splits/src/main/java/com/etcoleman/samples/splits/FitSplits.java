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
import java.util.Iterator;

/**
 * Created by etcoleman on 4/18/15.
 */
public class FitSplits {

  private final static Logger log = LoggerFactory.getLogger(FitSplits.class);

  // inputs
  private final int numDesiredBins;
  private final int totalRows;
  private final double error;
  private Iterator<Text> currentSplits;

  // results
  private ArrayList<RowCounter> calculated;

  // internally used.
  private int binSize;

  private NearestSplits nearestSplits;
  private int currentRowSum = 0;

  /**
   * Calculate splits based on desired number of splits and total number of rows to be processed. Uses the current splits (returned using Accumulo
   * tableOperations listSplits()) when they are close. Because of this there may be more splits than the desired number of splits. The objective calculate
   * splits for Accumulo bulk ingest, using few splits than the current table (generating larger files) while splitting on current table splits. </p> The
   * processing is performed on row id and number of occurrences. It is expected that bulk input data will be preprocessed to collapse rows that have the same
   * row and and to calculate the total number of rows.
   * 
   * @param numDesiredSplits
   *          target number of desired splits.
   * @param totalRows
   *          the expected total number of rows
   * @param error
   *          A factor 0 < x <= 1 used to control internal memory requirements by reducing fidelity of tracked rows. Expected values are 0.1 (10%) for tables
   *          with large number of splits.
   * @param currentSplits
   *          the current table splits from Accumulo tableOperations listSplits() method.
   */
  public FitSplits(final int numDesiredSplits, final int totalRows, double error, final Collection<Text> currentSplits) {

    if (currentSplits.size() < 1) {
      throw new IllegalStateException("At least one split must be provided.");
    }

    this.numDesiredBins = numDesiredSplits;
    this.totalRows = totalRows;
    this.error = error;

    binSize = (totalRows + 1) / (numDesiredSplits);

    calculated = new ArrayList<RowCounter>(numDesiredSplits + 2);

    this.currentSplits = currentSplits.iterator();

    nearestSplits = getNewSplit();

    log.trace("Nearest start {}", nearestSplits);
  }

  /**
   * Get the splits that have been calculated after processing rows with {@link #processRow(RowCounter)} and {@link #closeProcessing()}
   * 
   * @return the calculated splits
   */
  public Collection<RowCounter> getCalculatedSplits() {
    return calculated;
  }

  /**
   * Get splits from current table splits. First split is the lower bound, second split is the upper bound.
   * 
   * @return splits from current table splits.
   */
  private NearestSplits getNewSplit() {

    Text lower = nextSplit();
    Text upper = nextSplit();

    NearestSplits nearestSplits = new NearestSplits(error, binSize, lower, upper);

    return nearestSplits;
  }

  /**
   * Update the splits, setting upper split to lower and reading new split from current table splits.
   * 
   * @return splits from current table splits.
   */
  private NearestSplits getUpdatedSplit() {

    Text lower = nearestSplits.getUpperSplit();
    Text upper = nextSplit();

    NearestSplits nearestSplits = new NearestSplits(error, binSize, lower, upper);

    nearestSplits.clearLowerBoundDistance();

    return nearestSplits;
  }

  /**
   * Get the next split from the current table splits if available. If no more splits remain, null is returned.
   * 
   * @return the next split from current table splits or null if none available.
   */
  private Text nextSplit() {

    if (currentSplits.hasNext()) {
      return currentSplits.next();
    }

    return null;

  }

  /**
   * Processes the row id and the number of occurrences.
   * 
   * @param row
   *          row id and number of occurrences.
   */
  public void processRow(final RowCounter row) {

    currentRowSum += row.getCount();

    boolean atLowerSplit = nearestSplits.atLowerSplit(row);

    if (atLowerSplit && (currentRowSum >= binSize)) {

      log.debug("{} <-- matched split past boundary", row.getRowId());

      calculated.add(new RowCounter(row.getRowId(), currentRowSum));

      nearestSplits.clearSum();
      nearestSplits.clearCurrentRows();

      currentRowSum = 0;

      return;
    }

    boolean atUpperSplit = nearestSplits.atNextUpperSplit(row);

    // look for a new, lower split.
    if (atUpperSplit && (currentRowSum < binSize)) {

      nearestSplits = getUpdatedSplit();

      nearestSplits.setSum(currentRowSum);

      log.trace("new split {}", nearestSplits);

      return;
    }

    // if split suggested and at a given or past split - then split.
    if (atUpperSplit && (currentRowSum >= binSize)) {

      log.debug("{} <-- matched split", row.getRowId());

      calculated.add(new RowCounter(row.getRowId(), currentRowSum));

      currentRowSum = 0;
      nearestSplits = getNewSplit();

      return;
    }

    log.info("? current count {}, lowerBound distance {}", currentRowSum, nearestSplits.getLowerBoundDistance());

    if ((currentRowSum >= binSize) && (nearestSplits.getLowerBoundDistance() <= (binSize * error))) {
      log.error("use old.");

      int rowsUsed = currentRowSum - nearestSplits.getLowerBoundDistance();

      calculated.add(new RowCounter(nearestSplits.getLowerSplit(), rowsUsed));
      currentRowSum -= rowsUsed;
      nearestSplits = getNewSplit();
      nearestSplits.setSum(currentRowSum);
      return;
    }

    // force split if > 1.5 past ideal split.
    if (currentRowSum >= (binSize * 1.5)) {

      log.trace("past split boundary curr {} prev {}", currentRowSum, nearestSplits.getLowerBoundDistance());

      RowCounter last = null;
      int pastSum = 0;
      boolean usedSplit = false;

      for (RowCounter past : nearestSplits.getCurrentRows()) {

        log.debug("past is {}", past);

        last = past;
        pastSum += past.getCount();

        if (!usedSplit && pastSum >= binSize) {
          log.debug("{} <-- force split", past.getRowId());

          calculated.add(new RowCounter(past.getRowId(), pastSum));

          usedSplit = true;

          currentRowSum -= pastSum;
          if (currentRowSum < 0) {
            currentRowSum = 0;
          }

          pastSum = 0;
        }

      }

      if (last != null) {
        log.trace("Left over {} {}", pastSum, last.getRowId());
        nearestSplits.updateLowerSplit(last.getRowId(), pastSum);
      } else {
        nearestSplits.clearSum();
      }
    }

  }

  /**
   * nearestSplits may contain additional spilts at end - call this method to drain and process that buffer.
   */
  public void closeProcessing() {

    log.trace("CLOSING HAVE {}", nearestSplits.getCurrentRows().size());

    int pastSum = 0;

    for (RowCounter past : nearestSplits.getCurrentRows()) {

      log.trace("CLOSING {}", pastSum);

      pastSum += past.getCount();

      if (pastSum >= binSize) {
        calculated.add(new RowCounter(past.getRowId(), pastSum));
        pastSum = 0;
      }
    }
  }

}
