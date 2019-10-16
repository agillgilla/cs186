package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;

class SortMergeOperator extends JoinOperator {
    SortMergeOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new SortMergeIterator();
    }

    @Override
    public int estimateIOCost() {
        //does nothing
        return 0;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     *
     * Before proceeding, you should read and understand SNLJOperator.java
     *    You can find it in the same directory as this file.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given (Once again,
     *    SNLJOperator.java might be a useful reference).
     *
     */
    private class SortMergeIterator extends JoinIterator {
        /**
        * Some member variables are provided for guidance, but there are many possible solutions.
        * You should implement the solution that's best for you, using any member variables you need.
        * You're free to use these member variables, but you're not obligated to.
        */
        private BacktrackingIterator<Record> leftIterator;
        private BacktrackingIterator<Record> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private boolean marked;

        private SortMergeIterator() {
            super();
            // TODO(hw3_part1): implement

            this.nextRecord = null;

            SortOperator sortOperatorLeftRelation = new SortOperator(getTransaction(), getLeftTableName(), new LeftRecordComparator());
            SortOperator sortOperatorRightRelation = new SortOperator(getTransaction(), getRightTableName(), new RightRecordComparator());

            // Set each iterator to the sorted iterators from SortOperator
            leftIterator = getRecordIterator(sortOperatorLeftRelation.sort());
            rightIterator = getRecordIterator(sortOperatorRightRelation.sort());

            this.leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
            this.rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;

            try {
                fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
        }

        /**
         * Advances the left record
         *
         * The thrown exception means we're done: there is no next record
         * It causes this.fetchNextRecord (the caller) to hand control to its caller.
         */
        private void advanceLeftRecord() {
            if (!leftIterator.hasNext()) {
                throw new NoSuchElementException("All Done!");
            }
            this.leftRecord = leftIterator.next();
        }

        private void advanceRightRecord() {
            if (!rightIterator.hasNext()) {
                throw new NoSuchElementException("All Done!");
            }
            this.rightRecord = rightIterator.next();
        }

        private int compareLeftRightRecords(Record leftRecord, Record rightRecord) {
            return leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                    rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
        }

        /**
         * Pre-fetches what will be the next record, and puts it in this.nextRecord.
         * Pre-fetching simplifies the logic of this.hasNext() and this.next()
         */
        private void fetchNextRecord() {

            // Begin pseudocode
            /*
            if (!marked) {
                while (leftRecord < rightRecord) { advance leftRecord }
                while (leftRecord > rightRecord) { advance rightRecord }

                mark = rightRecord
            }

            if (leftRecord == rightRecord) {
                //rightRecord.getValues().get(getRightColumnIndex()).compareTo()
                result = <leftRecord, rightRecord>;

                advance rightRecord;

                return result
            } else {
               reset rightRecord to mark;

               advance leftRecord;

               mark = NULL;
            }
            */
            // End pseudocode


            if (this.leftRecord == null) { throw new NoSuchElementException("No new record to fetch"); }

            this.nextRecord = null;

            do {
                if (!marked) {
                    while (compareLeftRightRecords(leftRecord, rightRecord) < 0) {
                        advanceLeftRecord();
                    }

                    while (compareLeftRightRecords(leftRecord, rightRecord) > 0) {
                        advanceRightRecord();
                    }

                    this.marked = true;
                    this.rightIterator.markPrev();
                }

                DataBox leftJoinValue = this.leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex());
                DataBox rightJoinValue = this.rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex());
                if (leftJoinValue.equals(rightJoinValue)) {
                    List<DataBox> leftValues = new ArrayList<>(this.leftRecord.getValues());
                    List<DataBox> rightValues = new ArrayList<>(this.rightRecord.getValues());
                    leftValues.addAll(rightValues);

                    this.nextRecord = new Record(leftValues);

                    try {
                        advanceRightRecord();
                    } catch (NoSuchElementException e) {
                        rightIterator.reset();
                        advanceRightRecord();

                        try {
                            advanceLeftRecord();
                        } catch (NoSuchElementException ex) {
                            leftRecord = null;
                        }
                    }
                } else {
                    this.rightIterator.reset();
                    advanceRightRecord();

                    advanceLeftRecord();

                    this.marked = false;
                }

            } while (!hasNext());
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            // TODO(hw3_part1): implement

            return nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            // TODO(hw3_part1): implement

            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }

            Record nextRecord = this.nextRecord;
            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }

            return nextRecord;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private class LeftRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
            }
        }

        private class RightRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }
    }
}
