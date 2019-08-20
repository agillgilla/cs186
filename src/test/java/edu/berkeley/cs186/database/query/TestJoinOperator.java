package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.*;
import edu.berkeley.cs186.database.categories.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

import edu.berkeley.cs186.database.databox.BoolDataBox;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.FloatDataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.databox.StringDataBox;
import edu.berkeley.cs186.database.table.Record;

import org.junit.experimental.categories.Category;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import static org.junit.Assert.*;

@Category({HW3Tests.class, HW3Part1Tests.class})
public class TestJoinOperator {
    private Database d;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setup() throws IOException {
        File tempDir = tempFolder.newFolder("joinTest");
        d = new Database(tempDir.getAbsolutePath(), 32);
        d.setWorkMem(5); // B=5
    }

    @After
    public void cleanup() {
        d.close();
    }

    // 4 second max per method tested.
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (
                4000 * TimeoutScaling.factor)));

    @Test
    @Category(PublicTests.class)
    public void testSimpleJoinPNLJ() {
        TestSourceOperator sourceOperator = new TestSourceOperator();
        try(Transaction transaction = d.beginTransaction()) {
            JoinOperator joinOperator = new PNLJOperator(sourceOperator, sourceOperator, "int", "int",
                    transaction.getTransactionContext());

            Iterator<Record> outputIterator = joinOperator.iterator();
            int numRecords = 0;

            List<DataBox> expectedRecordValues = new ArrayList<>();
            expectedRecordValues.add(new BoolDataBox(true));
            expectedRecordValues.add(new IntDataBox(1));
            expectedRecordValues.add(new StringDataBox("a", 1));
            expectedRecordValues.add(new FloatDataBox(1.2f));
            expectedRecordValues.add(new BoolDataBox(true));
            expectedRecordValues.add(new IntDataBox(1));
            expectedRecordValues.add(new StringDataBox("a", 1));
            expectedRecordValues.add(new FloatDataBox(1.2f));
            Record expectedRecord = new Record(expectedRecordValues);

            while (outputIterator.hasNext() && numRecords < 100 * 100) {
                assertEquals("mismatch at record " + numRecords, expectedRecord, outputIterator.next());
                numRecords++;
            }

            assertFalse("too many records", outputIterator.hasNext());
            assertEquals("too few records", 100 * 100, numRecords);
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleJoinBNLJ() {
        TestSourceOperator sourceOperator = new TestSourceOperator();
        d.setWorkMem(5); // B=5
        try(Transaction transaction = d.beginTransaction()) {
            JoinOperator joinOperator = new BNLJOperator(sourceOperator, sourceOperator, "int", "int",
                    transaction.getTransactionContext());

            Iterator<Record> outputIterator = joinOperator.iterator();
            int numRecords = 0;

            List<DataBox> expectedRecordValues = new ArrayList<>();
            expectedRecordValues.add(new BoolDataBox(true));
            expectedRecordValues.add(new IntDataBox(1));
            expectedRecordValues.add(new StringDataBox("a", 1));
            expectedRecordValues.add(new FloatDataBox(1.2f));
            expectedRecordValues.add(new BoolDataBox(true));
            expectedRecordValues.add(new IntDataBox(1));
            expectedRecordValues.add(new StringDataBox("a", 1));
            expectedRecordValues.add(new FloatDataBox(1.2f));
            Record expectedRecord = new Record(expectedRecordValues);

            while (outputIterator.hasNext() && numRecords < 100 * 100) {
                assertEquals("mismatch at record " + numRecords, expectedRecord, outputIterator.next());
                numRecords++;
            }

            assertFalse("too many records", outputIterator.hasNext());
            assertEquals("too few records", 100 * 100, numRecords);
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testSimplePNLJOutputOrder() {
        try(Transaction transaction = d.beginTransaction()) {
            Record r1 = TestUtils.createRecordWithAllTypesWithValue(1);
            List<DataBox> r1Vals = r1.getValues();
            Record r2 = TestUtils.createRecordWithAllTypesWithValue(2);
            List<DataBox> r2Vals = r2.getValues();

            List<DataBox> expectedRecordValues1 = new ArrayList<>();
            List<DataBox> expectedRecordValues2 = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                expectedRecordValues1.addAll(r1Vals);
                expectedRecordValues2.addAll(r2Vals);
            }

            Record expectedRecord1 = new Record(expectedRecordValues1);
            Record expectedRecord2 = new Record(expectedRecordValues2);
            transaction.createTable(TestUtils.createSchemaWithAllTypes(), "leftTable");
            transaction.createTable(TestUtils.createSchemaWithAllTypes(), "rightTable");

            for (int i = 0; i < 400; i++) {
                List<DataBox> vals;
                if (i < 200) {
                    vals = r1Vals;
                } else {
                    vals = r2Vals;
                }
                transaction.getTransactionContext().addRecord("leftTable", vals);
                transaction.getTransactionContext().addRecord("rightTable", vals);
            }

            for (int i = 0; i < 400; i++) {
                if (i < 200) {
                    transaction.getTransactionContext().addRecord("leftTable", r2Vals);
                    transaction.getTransactionContext().addRecord("rightTable", r1Vals);
                } else {
                    transaction.getTransactionContext().addRecord("leftTable", r1Vals);
                    transaction.getTransactionContext().addRecord("rightTable", r2Vals);
                }
            }

            QueryOperator s1 = new SequentialScanOperator(transaction.getTransactionContext(), "leftTable");
            QueryOperator s2 = new SequentialScanOperator(transaction.getTransactionContext(), "rightTable");
            QueryOperator joinOperator = new PNLJOperator(s1, s2, "int", "int",
                    transaction.getTransactionContext());

            int count = 0;
            Iterator<Record> outputIterator = joinOperator.iterator();

            while (outputIterator.hasNext() && count < 400 * 400 * 2) {
                if (count < 200 * 200) {
                    assertEquals("mismatch at record " + count, expectedRecord1, outputIterator.next());
                } else if (count < 200 * 200 * 2) {
                    assertEquals("mismatch at record " + count, expectedRecord2, outputIterator.next());
                } else if (count < 200 * 200 * 3) {
                    assertEquals("mismatch at record " + count, expectedRecord1, outputIterator.next());
                } else if (count < 200 * 200 * 4) {
                    assertEquals("mismatch at record " + count, expectedRecord2, outputIterator.next());
                } else if (count < 200 * 200 * 5) {
                    assertEquals("mismatch at record " + count, expectedRecord2, outputIterator.next());
                } else if (count < 200 * 200 * 6) {
                    assertEquals("mismatch at record " + count, expectedRecord1, outputIterator.next());
                } else if (count < 200 * 200 * 7) {
                    assertEquals("mismatch at record " + count, expectedRecord2, outputIterator.next());
                } else {
                    assertEquals("mismatch at record " + count, expectedRecord1, outputIterator.next());
                }
                count++;
            }

            assertFalse("too many records", outputIterator.hasNext());
            assertEquals("too few records", 400 * 400 * 2, count);
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleSortMergeJoin() {
        TestSourceOperator sourceOperator = new TestSourceOperator();
        d.setWorkMem(5); // B=5
        try(Transaction transaction = d.beginTransaction()) {
            JoinOperator joinOperator = new SortMergeOperator(sourceOperator, sourceOperator, "int", "int",
                    transaction.getTransactionContext());

            Iterator<Record> outputIterator = joinOperator.iterator();
            int numRecords = 0;

            List<DataBox> expectedRecordValues = new ArrayList<>();
            expectedRecordValues.add(new BoolDataBox(true));
            expectedRecordValues.add(new IntDataBox(1));
            expectedRecordValues.add(new StringDataBox("a", 1));
            expectedRecordValues.add(new FloatDataBox(1.2f));
            expectedRecordValues.add(new BoolDataBox(true));
            expectedRecordValues.add(new IntDataBox(1));
            expectedRecordValues.add(new StringDataBox("a", 1));
            expectedRecordValues.add(new FloatDataBox(1.2f));
            Record expectedRecord = new Record(expectedRecordValues);

            while (outputIterator.hasNext() && numRecords < 100 * 100) {
                assertEquals("mismatch at record " + numRecords, expectedRecord, outputIterator.next());
                numRecords++;
            }

            assertFalse("too many records", outputIterator.hasNext());
            assertEquals("too few records", 100 * 100, numRecords);
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testSortMergeJoinUnsortedInputs()  {
        d.setWorkMem(3); // B=3
        try(Transaction transaction = d.beginTransaction()) {
            transaction.createTable(TestUtils.createSchemaWithAllTypes(), "leftTable");
            transaction.createTable(TestUtils.createSchemaWithAllTypes(), "rightTable");
            Record r1 = TestUtils.createRecordWithAllTypesWithValue(1);
            List<DataBox> r1Vals = r1.getValues();
            Record r2 = TestUtils.createRecordWithAllTypesWithValue(2);
            List<DataBox> r2Vals = r2.getValues();
            Record r3 = TestUtils.createRecordWithAllTypesWithValue(3);
            List<DataBox> r3Vals = r3.getValues();
            Record r4 = TestUtils.createRecordWithAllTypesWithValue(4);
            List<DataBox> r4Vals = r4.getValues();
            List<DataBox> expectedRecordValues1 = new ArrayList<>();
            List<DataBox> expectedRecordValues2 = new ArrayList<>();
            List<DataBox> expectedRecordValues3 = new ArrayList<>();
            List<DataBox> expectedRecordValues4 = new ArrayList<>();

            for (int i = 0; i < 2; i++) {
                expectedRecordValues1.addAll(r1Vals);
                expectedRecordValues2.addAll(r2Vals);
                expectedRecordValues3.addAll(r3Vals);
                expectedRecordValues4.addAll(r4Vals);
            }
            Record expectedRecord1 = new Record(expectedRecordValues1);
            Record expectedRecord2 = new Record(expectedRecordValues2);
            Record expectedRecord3 = new Record(expectedRecordValues3);
            Record expectedRecord4 = new Record(expectedRecordValues4);
            List<Record> leftTableRecords = new ArrayList<>();
            List<Record> rightTableRecords = new ArrayList<>();
            for (int i = 0; i < 400 * 2; i++) {
                Record r;
                if (i % 4 == 0) {
                    r = r1;
                } else if (i % 4 == 1) {
                    r = r2;
                } else if (i % 4 == 2) {
                    r = r3;
                } else {
                    r = r4;
                }
                leftTableRecords.add(r);
                rightTableRecords.add(r);
            }
            Collections.shuffle(leftTableRecords, new Random(10));
            Collections.shuffle(rightTableRecords, new Random(20));
            for (int i = 0; i < 400 * 2; i++) {
                transaction.getTransactionContext().addRecord("leftTable", leftTableRecords.get(i).getValues());
                transaction.getTransactionContext().addRecord("rightTable", rightTableRecords.get(i).getValues());
            }

            QueryOperator s1 = new SequentialScanOperator(transaction.getTransactionContext(), "leftTable");
            QueryOperator s2 = new SequentialScanOperator(transaction.getTransactionContext(), "rightTable");

            JoinOperator joinOperator = new SortMergeOperator(s1, s2, "int", "int",
                    transaction.getTransactionContext());

            Iterator<Record> outputIterator = joinOperator.iterator();
            int numRecords = 0;
            Record expectedRecord;

            while (outputIterator.hasNext() && numRecords < 400 * 400) {
                if (numRecords < (400 * 400 / 4)) {
                    expectedRecord = expectedRecord1;
                } else if (numRecords < (400 * 400 / 2)) {
                    expectedRecord = expectedRecord2;
                } else if (numRecords < 400 * 400 - (400 * 400 / 4)) {
                    expectedRecord = expectedRecord3;
                } else {
                    expectedRecord = expectedRecord4;
                }
                Record r = outputIterator.next();
                assertEquals("mismatch at record " + numRecords, r, expectedRecord);
                numRecords++;
            }

            assertFalse("too many records", outputIterator.hasNext());
            assertEquals("too few records", 400 * 400, numRecords);
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testBNLJDiffOutPutThanPNLJ() {
        d.setWorkMem(4); // B=4
        try(Transaction transaction = d.beginTransaction()) {
            Record r1 = TestUtils.createRecordWithAllTypesWithValue(1);
            List<DataBox> r1Vals = r1.getValues();
            Record r2 = TestUtils.createRecordWithAllTypesWithValue(2);
            List<DataBox> r2Vals = r2.getValues();
            Record r3 = TestUtils.createRecordWithAllTypesWithValue(3);
            List<DataBox> r3Vals = r3.getValues();
            Record r4 = TestUtils.createRecordWithAllTypesWithValue(4);
            List<DataBox> r4Vals = r4.getValues();
            List<DataBox> expectedRecordValues1 = new ArrayList<>();
            List<DataBox> expectedRecordValues2 = new ArrayList<>();
            List<DataBox> expectedRecordValues3 = new ArrayList<>();
            List<DataBox> expectedRecordValues4 = new ArrayList<>();

            for (int i = 0; i < 2; i++) {
                expectedRecordValues1.addAll(r1Vals);
                expectedRecordValues2.addAll(r2Vals);
                expectedRecordValues3.addAll(r3Vals);
                expectedRecordValues4.addAll(r4Vals);
            }
            Record expectedRecord1 = new Record(expectedRecordValues1);
            Record expectedRecord2 = new Record(expectedRecordValues2);
            Record expectedRecord3 = new Record(expectedRecordValues3);
            Record expectedRecord4 = new Record(expectedRecordValues4);
            transaction.createTable(TestUtils.createSchemaWithAllTypes(), "leftTable");
            transaction.createTable(TestUtils.createSchemaWithAllTypes(), "rightTable");
            for (int i = 0; i < 2 * 400; i++) {
                if (i < 200) {
                    transaction.getTransactionContext().addRecord("leftTable", r1Vals);
                    transaction.getTransactionContext().addRecord("rightTable", r3Vals);
                } else if (i < 400) {
                    transaction.getTransactionContext().addRecord("leftTable", r2Vals);
                    transaction.getTransactionContext().addRecord("rightTable", r4Vals);
                } else if (i < 600) {
                    transaction.getTransactionContext().addRecord("leftTable", r3Vals);
                    transaction.getTransactionContext().addRecord("rightTable", r1Vals);
                } else {
                    transaction.getTransactionContext().addRecord("leftTable", r4Vals);
                    transaction.getTransactionContext().addRecord("rightTable", r2Vals);
                }
            }
            QueryOperator s1 = new SequentialScanOperator(transaction.getTransactionContext(), "leftTable");
            QueryOperator s2 = new SequentialScanOperator(transaction.getTransactionContext(), "rightTable");
            QueryOperator joinOperator = new BNLJOperator(s1, s2, "int", "int",
                    transaction.getTransactionContext());
            Iterator<Record> outputIterator = joinOperator.iterator();
            int count = 0;
            while (outputIterator.hasNext() && count < 4 * 200 * 200) {
                Record r = outputIterator.next();
                if (count < 200 * 200) {
                    assertEquals("mismatch at record " + count, expectedRecord3, r);
                } else if (count < 2 * 200 * 200) {
                    assertEquals("mismatch at record " + count, expectedRecord4, r);
                } else if (count < 3 * 200 * 200) {
                    assertEquals("mismatch at record " + count, expectedRecord1, r);
                } else {
                    assertEquals("mismatch at record " + count, expectedRecord2, r);
                }
                count++;
            }
            assertFalse("too many records", outputIterator.hasNext());
            assertEquals("too few records", 4 * 200 * 200, count);
        }
    }
}
