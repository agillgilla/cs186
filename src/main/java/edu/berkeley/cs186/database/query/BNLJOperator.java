package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.table.Record;

class BNLJOperator extends JoinOperator {
    protected int numBuffers;

    BNLJOperator(QueryOperator leftSource,
                 QueryOperator rightSource,
                 String leftColumnName,
                 String rightColumnName,
                 TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.BNLJ);

        this.numBuffers = transaction.getWorkMemSize();

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new BNLJIterator();
    }

    @Override
    public int estimateIOCost() {
        //This method implements the the IO cost estimation of the Block Nested Loop Join
        int usableBuffers = numBuffers - 2;
        int numLeftPages = getLeftSource().getStats().getNumPages();
        int numRightPages = getRightSource().getStats().getNumPages();
        return ((int) Math.ceil((double) numLeftPages / (double) usableBuffers)) * numRightPages +
               numLeftPages;
    }

    /**
     * BNLJ: Block Nested Loop Join
     *  See lecture slides.
     *
     * An implementation of Iterator that provides an iterator interface for this operator.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given.
     */
    private class BNLJIterator extends JoinIterator {
        // Iterator over pages of the left relation
        private BacktrackingIterator<Page> leftIterator;
        // Iterator over pages of the right relation
        private BacktrackingIterator<Page> rightIterator;
        // Iterator over records in the current block of left pages
        private BacktrackingIterator<Record> leftRecordIterator = null;
        // Iterator over records in the current right page
        private BacktrackingIterator<Record> rightRecordIterator = null;
        // The current record on the left page
        private Record leftRecord = null;
        // The next record to return
        private Record nextRecord = null;

        private BNLJIterator() {
            super();

            this.leftIterator = BNLJOperator.this.getPageIterator(this.getLeftTableName());
            fetchNextLeftBlock();

            this.rightIterator = BNLJOperator.this.getPageIterator(this.getRightTableName());
            this.rightIterator.markNext();
            fetchNextRightPage();

            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
        }

        /**
         * Fetch the next non-empty block of B - 2 pages from the left relation. leftRecordIterator
         * should be set to a record iterator over the next B - 2 pages of the left relation that
         * have a record in them, and leftRecord should be set to the first record in this block.
         *
         * If there are no more pages in the left relation with records, both leftRecordIterator
         * and leftRecord should be set to null.
         */
        private void fetchNextLeftBlock() {
            // TODO(hw3_part1): implement

            while (true) {
                this.leftIterator.markNext();
                Iterator<Record> currIterator = getBlockIterator(this.getLeftTableName(), this.leftIterator, 1);

                if (currIterator.hasNext()) {
                    leftIterator.reset();
                    leftRecordIterator = getBlockIterator(this.getLeftTableName(), this.leftIterator, numBuffers - 2);
                    leftRecordIterator.markNext();
                    leftRecord = leftRecordIterator.next();
                    return;
                }

                if (this.leftIterator.hasNext()) {
                    this.leftIterator.next();
                } else {
                    leftRecordIterator = null;
                    leftRecord = null;
                    return;
                }
            }
        }

        /**
         * Fetch the next non-empty page from the right relation. rightRecordIterator
         * should be set to a record iterator over the next page of the right relation that
         * has a record in it.
         *
         * If there are no more pages in the left relation with records, rightRecordIterator
         * should be set to null.
         */
        private boolean fetchNextRightPage() {
            // TODO(hw3_part1): implement

            boolean looped = false;

            while (true) {
                BacktrackingIterator<Record> currIterator = getBlockIterator(this.getRightTableName(), this.rightIterator, 1);

                if (currIterator.hasNext()) {
                    rightRecordIterator = currIterator;
                    rightRecordIterator.markNext();
                    return looped;
                }

                if (this.rightIterator.hasNext()) {
                    this.rightIterator.next();
                } else {
                    looped = true;
                    this.rightIterator.reset();
                }
            }
        }

        // This function causes an I/O so I was lazy and used !rightIterator.hasNext()
        private boolean isAtEndOfRightRelation() {
            while (true) {
                BacktrackingIterator<Record> currIterator = getBlockIterator(this.getRightTableName(), this.rightIterator, 1);

                if (currIterator.hasNext()) {
                    return false;
                }

                if (this.rightIterator.hasNext()) {
                    this.rightIterator.next();
                } else {
                    this.rightIterator.reset();
                    return true;
                }
            }
        }

        /**
         * Fetches the next record to return, and sets nextRecord to it. If there are no more
         * records to return, a NoSuchElementException should be thrown.
         *
         * @throws NoSuchElementException if there are no more Records to yield
         */
        private void fetchNextRecord() {
            // TODO(hw3_part1): implement

            if (this.leftRecord == null) {
                throw new NoSuchElementException("No new record to fetch");
            }
            this.nextRecord = null;

            Record rightRecord = null;

            do {
                if (rightRecordIterator.hasNext()) {
                    rightRecord = this.rightRecordIterator.next();
                } else {
                    // We just reached the end of a page in the right relation
                    if (leftRecordIterator.hasNext()) {
                        // We have more records in the left relation (iterate left and right)
                        this.leftRecord = this.leftRecordIterator.next();

                        // Reset the page in the right relation for looping
                        this.rightRecordIterator.reset();
                        this.rightRecordIterator.markNext();
                        rightRecord = this.rightRecordIterator.next();
                    } else {
                        // We have just looped through all the records in B-2 block of left relation

                        // TODO: How can I move this to before fetching the next right page?
                        // TODO: i.e. how do you check if you'll need to fetch a new block before getting next right page?
                        if (!rightIterator.hasNext()) {
                            // We looped around in the right relation page loop;
                            // we need to fetch a new B-2 block from the left relation
                            fetchNextLeftBlock();
                            if (leftRecord == null) {
                                throw new NoSuchElementException("No more records in left relation");
                            }
                        } else {
                            // We started a new page loop on right relation, but
                            // still midway through all of right relation's records
                            leftRecordIterator.reset();
                            leftRecordIterator.markNext();
                            leftRecord = leftRecordIterator.next();
                        }

                        fetchNextRightPage();

                        this.rightRecordIterator.markNext();
                        rightRecord = this.rightRecordIterator.next();
                    }
                }

                DataBox leftJoinValue = this.leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
                DataBox rightJoinValue = rightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());

                if (leftJoinValue.equals(rightJoinValue)) {
                    this.nextRecord = joinRecords(leftRecord, rightRecord);
                }

            } while (nextRecord == null);
        }

        /**
         * Helper method to create a joined record from a record of the left relation
         * and a record of the right relation.
         * @param leftRecord Record from the left relation
         * @param rightRecord Record from the right relation
         * @return joined record
         */
        private Record joinRecords(Record leftRecord, Record rightRecord) {
            List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            return new Record(leftValues);
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            return this.nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
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
    }
}
