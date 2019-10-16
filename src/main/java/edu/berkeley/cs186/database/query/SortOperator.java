package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.memory.Page;

import java.util.*;

public class SortOperator {
    private TransactionContext transaction;
    private String tableName;
    private Comparator<Record> comparator;
    private Schema operatorSchema;
    private int numBuffers;
    private String sortedTableName = null;

    public SortOperator(TransactionContext transaction, String tableName,
                        Comparator<Record> comparator) {
        this.transaction = transaction;
        this.tableName = tableName;
        this.comparator = comparator;
        this.operatorSchema = this.computeSchema();
        this.numBuffers = this.transaction.getWorkMemSize();
    }

    private Schema computeSchema() {
        try {
            return this.transaction.getFullyQualifiedSchema(this.tableName);
        } catch (DatabaseException de) {
            throw new QueryPlanException(de);
        }
    }

    /**
     * Interface for a run. Also see createRun/createRunFromIterator.
     */
    public interface Run extends Iterable<Record> {
        /**
         * Add a record to the run.
         * @param values set of values of the record to add to run
         */
        void addRecord(List<DataBox> values);

        /**
         * Add a list of records to the run.
         * @param records records to add to the run
         */
        void addRecords(List<Record> records);

        @Override
        Iterator<Record> iterator();

        /**
         * Table name of table backing the run.
         * @return table name
         */
        String tableName();
    }

    /**
     * Returns a NEW run that is the sorted version of the input run.
     * Can do an in memory sort over all the records in this run
     * using one of Java's built-in sorting methods.
     * Note: Don't worry about modifying the original run.
     * Returning a new run would bring one extra page in memory beyond the
     * size of the buffer, but it is done this way for ease.
     */
    public Run sortRun(Run run) {
        // TODO(hw3_part1): implement

        Iterator<Record> runIterator = run.iterator();
        ArrayList<Record> recordList = new ArrayList<>();

        while (runIterator.hasNext()) {
            recordList.add(runIterator.next());
        }

        recordList.sort(this.comparator);

        Run sortedRun = createRun();
        sortedRun.addRecords(recordList);

        return sortedRun;
    }

    /**
     * Given a list of sorted runs, returns a new run that is the result
     * of merging the input runs. You should use a Priority Queue (java.util.PriorityQueue)
     * to determine which record should be should be added to the output run next.
     * It is recommended that your Priority Queue hold Pair<Record, Integer> objects
     * where a Pair (r, i) is the Record r with the smallest value you are
     * sorting on currently unmerged from run i.
     */
    public Run mergeSortedRuns(List<Run> runs) {
        // TODO(hw3_part1): implement

        PriorityQueue<Pair<Record, Integer>> runPriorityQueue = new PriorityQueue<>(runs.size(), new RecordPairComparator());

        Run mergedRun = createRun();

        Iterator<Record>[] runIterators = new Iterator[runs.size()];

        for (int i = 0; i < runs.size(); i++) {
            runIterators[i] = runs.get(i).iterator();
            if (runIterators[i].hasNext()) {
                runPriorityQueue.add(new Pair(runIterators[i].next(), i));
            }
        }

        while (!runPriorityQueue.isEmpty()) {
            Pair minRecordPair = runPriorityQueue.poll();

            mergedRun.addRecord(((Record) minRecordPair.getFirst()).getValues());

            int iteratorIndex = (int) minRecordPair.getSecond();

            if (runIterators[iteratorIndex].hasNext()) {
                runPriorityQueue.add(new Pair(runIterators[iteratorIndex].next(), iteratorIndex));
            }
        }

        return mergedRun;
    }

    /**
     * Given a list of N sorted runs, returns a list of
     * sorted runs that is the result of merging (numBuffers - 1)
     * of the input runs at a time.
     */
    public List<Run> mergePass(List<Run> runs) {
        // TODO(hw3_part1): implement

        List<Run> mergedRunsList = new ArrayList<>();

        List<Run> bufferedRuns;
        Run mergedBufferedRun;

        while (!runs.isEmpty()) {
            bufferedRuns = new ArrayList<>();

            for (int i = 0; i < this.numBuffers - 1; i++) {
                if (!runs.isEmpty()) {
                    bufferedRuns.add(runs.remove(0));
                }
            }

            mergedBufferedRun = mergeSortedRuns(bufferedRuns);
            mergedRunsList.add(mergedBufferedRun);
        }

        return mergedRunsList;
    }

    /**
     * Does an external merge sort on the table with name tableName
     * using numBuffers.
     * Returns the name of the table that backs the final run.
     */
    public String sort() {
        // TODO(hw3_part1): implement

        // If the table is empty, just return the current table
        if (this.transaction.getNumRecords(this.tableName) == 0) {
            return this.tableName;
        }

        BacktrackingIterator<Page> tablePageIterator = this.transaction.getPageIterator(this.tableName);

        List<Run> sortedRuns = new ArrayList<>();
        Run currRun;

        while (tablePageIterator.hasNext()) {
            BacktrackingIterator<Record> bufferedRecordIterator = this.transaction.getBlockIterator(this.tableName, tablePageIterator, this.numBuffers);

            // Create runs that are one block (numBuffers) long
            currRun = createRunFromIterator(bufferedRecordIterator);
            // Sort each run and add it to the list of sorted runs
            sortedRuns.add(sortRun(currRun));
        }

        // Keep merging until we have one final run
        while (sortedRuns.size() != 1) {
            sortedRuns = mergePass(sortedRuns);
        }

        Run finalRun = sortedRuns.get(0);

        return finalRun.tableName(); // TODO(hw3_part1): replace this!
    }

    public Iterator<Record> iterator() {
        if (sortedTableName == null) {
            sortedTableName = sort();
        }
        return this.transaction.getRecordIterator(sortedTableName);
    }

    /**
     * Creates a new run for intermediate steps of sorting. The created
     * run supports adding records.
     * @return a new, empty run
     */
    Run createRun() {
        return new IntermediateRun();
    }

    /**
     * Creates a run given a backtracking iterator of records. Record adding
     * is not supported, but creating this run will not incur any I/Os aside
     * from any I/Os incurred while reading from the given iterator.
     * @param records iterator of records
     * @return run backed by the iterator of records
     */
    Run createRunFromIterator(BacktrackingIterator<Record> records) {
        return new InputDataRun(records);
    }

    private class IntermediateRun implements Run {
        String tempTableName;

        IntermediateRun() {
            this.tempTableName = SortOperator.this.transaction.createTempTable(
                                     SortOperator.this.operatorSchema);
        }

        @Override
        public void addRecord(List<DataBox> values) {
            SortOperator.this.transaction.addRecord(this.tempTableName, values);
        }

        @Override
        public void addRecords(List<Record> records) {
            for (Record r : records) {
                this.addRecord(r.getValues());
            }
        }

        @Override
        public Iterator<Record> iterator() {
            return SortOperator.this.transaction.getRecordIterator(this.tempTableName);
        }

        @Override
        public String tableName() {
            return this.tempTableName;
        }
    }

    private static class InputDataRun implements Run {
        BacktrackingIterator<Record> iterator;

        InputDataRun(BacktrackingIterator<Record> iterator) {
            this.iterator = iterator;
            this.iterator.markPrev();
        }

        @Override
        public void addRecord(List<DataBox> values) {
            throw new UnsupportedOperationException("cannot add record to input data run");
        }

        @Override
        public void addRecords(List<Record> records) {
            throw new UnsupportedOperationException("cannot add records to input data run");
        }

        @Override
        public Iterator<Record> iterator() {
            iterator.reset();
            return iterator;
        }

        @Override
        public String tableName() {
            throw new UnsupportedOperationException("cannot get table name of input data run");
        }
    }

    private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
        @Override
        public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
            return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());
        }
    }
}

