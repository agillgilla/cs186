package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.TimeoutScaling;
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.categories.HW5Tests;
import edu.berkeley.cs186.database.categories.StudentTests;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.io.DiskSpaceManagerImpl;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.BufferManagerImpl;
import edu.berkeley.cs186.database.memory.LRUEvictionPolicy;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import java.util.*;
import java.util.function.Consumer;

import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.*;
import static org.junit.Assert.assertNotNull;

/**
 * File for student tests for HW5 (Recovery). Tests are run through
 * TestARIESStudentRunner for grading purposes.
 */
@Category({HW5Tests.class, StudentTests.class})
public class TestARIESStudent {
    private String testDir;
    private RecoveryManager recoveryManager;
    private final Queue<Consumer<LogRecord>> redoMethods = new ArrayDeque<>();

    // 1 second per test
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (
                1000 * TimeoutScaling.factor)));

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setup() throws Exception {
        testDir = tempFolder.newFolder("test-dir").getAbsolutePath();
        recoveryManager = loadRecoveryManager(testDir);
        DummyTransaction.cleanupTransactions();
        LogRecord.onRedoHandler(t -> {});
    }

    @After
    public void cleanup() throws Exception {}

    @Test
    public void testStudentAnalysis() throws Exception {
        // TODO(hw5): write your own test on restartAnalysis only
        // You should use loadRecoveryManager instead of new ARIESRecoveryManager(..) to
        // create the recovery manager, and use runAnalysis(inner) instead of
        // inner.restartAnalysis() to call the analysis routine.

        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF };

        LogManager logManager = getLogManager(recoveryManager);

        DummyTransaction transaction1 = DummyTransaction.create(getTransNum(1));
        DummyTransaction transaction2 = DummyTransaction.create(getTransNum(2));
        DummyTransaction transaction3 = DummyTransaction.create(getTransNum(3));

        List<Long> LSNs = new ArrayList<>();
        // LSN idx 0: T1 updates P3
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(getTransNum(1), getPageNum(3), 0L, (short) 0, before, after)));
        // LSN idx 1: T1 updates P1
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(getTransNum(1), getPageNum(1), LSNs.get(0), (short) 0, before, after)));
        // LSN idx 2: T2 updates P2
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(getTransNum(2), getPageNum(2), 0L, (short) 0, before, after)));
        // LSN idx 3: T3 updates P1
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(getTransNum(3), getPageNum(1), 0L, (short) 0, before, after)));
        // LSN idx 4: Begin Checkpoint
        LogRecord beginCkptRecord = new BeginCheckpointLogRecord(9876543210L);
        LSNs.add(logManager.appendToLog(beginCkptRecord));

        MasterLogRecord masterRecord = new MasterLogRecord(LSNs.get(LSNs.size() - 1));
        logManager.rewriteMasterRecord(masterRecord);

        // LSN idx 5: T3 updates P3
        LogRecord t3UpdateP3Record = new UpdatePageLogRecord(getTransNum(3), getPageNum(3), LSNs.get(3), (short) 0, before, after);
        LSNs.add(logManager.appendToLog(t3UpdateP3Record));
        // LSN idx 6: T3 aborts
        LSNs.add(logManager.appendToLog(new AbortTransactionLogRecord(getTransNum(3), LSNs.get(5))));

        // Initialize end checkpoint dpt, transaction table, and touched pages
        HashMap<Long, Long> ckptDirtyPageTable = new HashMap<>();
        ckptDirtyPageTable.put(getPageNum(1), LSNs.get(3));
        ckptDirtyPageTable.put(getPageNum(3), LSNs.get(0));

        HashMap<Long, Pair<Transaction.Status, Long>> ckptTransactionTable = new HashMap<>();
        ckptTransactionTable.put(getTransNum(1), new Pair<>(Transaction.Status.RUNNING, LSNs.get(0)));
        ckptTransactionTable.put(getTransNum(2), new Pair<>(Transaction.Status.RUNNING, LSNs.get(2)));
        ckptTransactionTable.put(getTransNum(3), new Pair<>(Transaction.Status.RUNNING, LSNs.get(3)));

        HashMap<Long, TransactionTableEntry> endCkptTransactionTable = new HashMap<>();
        endCkptTransactionTable.put(getTransNum(1), new TransactionTableEntry(transaction1));
        endCkptTransactionTable.put(getTransNum(2), new TransactionTableEntry(transaction2));
        endCkptTransactionTable.put(getTransNum(3), new TransactionTableEntry(transaction3));

        HashMap<Long, List<Long>> ckptTouchedPages = new HashMap<>();
        ckptTouchedPages.put(getTransNum(1), Arrays.asList(getPageNum(3), getPageNum(1)));
        ckptTouchedPages.put(getTransNum(2), Arrays.asList(getPageNum(2)));
        ckptTouchedPages.put(getTransNum(3), Arrays.asList(getPageNum(1), getPageNum(3)));

        for (long transNum : ckptTouchedPages.keySet()) {
            List<Long> touchedPages = ckptTouchedPages.get(transNum);
            for (Long touchedPage : touchedPages) {
                endCkptTransactionTable.get(transNum).touchedPages.add(touchedPage);
            }
        }

        //System.out.println("LSNs length (before end): " + LSNs.size());

        // LSN idx 7: End Checkpoint
        createEndCheckpointDataRecords(logManager, ckptDirtyPageTable, endCkptTransactionTable, beginCkptRecord.LSN, LSNs);

        //System.out.println("LSNs length (after end): " + LSNs.size());

        // Create CLR
        Pair<LogRecord, Boolean> CLRandFlushRequired = t3UpdateP3Record.undo(ckptTransactionTable.get(getTransNum(3)).getSecond());
        LogRecord CLR = CLRandFlushRequired.getFirst();

        // LSN idx 8: CLR undo T3 with LSN 60
        LSNs.add(logManager.appendToLog(CLR));
        // LSN idx 9: T1 updates P4
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(getTransNum(1), getPageNum(4), LSNs.get(6), (short) 0, before, after)));
        // LSN idx 10: T1 commits
        LSNs.add(logManager.appendToLog(new CommitTransactionLogRecord(getTransNum(1), LSNs.get(9))));
        // LSN idx 11: T1 ends
        LSNs.add(logManager.appendToLog(new EndTransactionLogRecord(getTransNum(1), LSNs.get(10))));

        //System.out.println("LSNs list:");
        for (int i = 0; i < LSNs.size(); i++) {
            //System.out.println(i + ": " + LSNs.get(i) + "\t (" + (i*10 + 10) + ")");
        }

        // flush everything - recovery tests should always start
        // with a clean load from disk, and here we want everything sent to disk first.
        // Note: this does not call RecoveryManager#close - it only closes the
        // buffer manager and disk space manager.
        shutdownRecoveryManager(recoveryManager);

        // load from disk again
        recoveryManager = loadRecoveryManager(testDir);

        // new recovery manager - tables/log manager/other state loaded with old manager are different
        // with the new recovery manager
        logManager = getLogManager(recoveryManager);
        Map<Long, Long> dirtyPageTable = getDirtyPageTable(recoveryManager);
        Map<Long, TransactionTableEntry> transactionTable = getTransactionTable(recoveryManager);

        runAnalysis(recoveryManager);

        //System.out.println("------------------------------------");
        //System.out.println("Actual dpt (after analysis)");
        for (long pageNum : dirtyPageTable.keySet()) {
            //System.out.println("P" + getInformalPageNum(pageNum) + ": recLSN - " + dirtyPageTable.get(pageNum));
        }
        //System.out.println("------------------------------------");
        //System.out.println("Actual transaction table (after analysis):");
        for (long transNum : transactionTable.keySet()) {
            //System.out.println("T" + transNum + ": " + transactionTable.get(transNum).transaction.getStatus() + ", " + transactionTable.get(transNum).lastLSN);
        }
        //System.out.println("------------------------------------");

        // Transaction table
        assertFalse(transactionTable.containsKey(getTransNum(1)));
        assertTrue(transactionTable.containsKey(getTransNum(2)));
        assertTrue(transactionTable.containsKey(getTransNum(3)));
        assertEquals((long) 10000, transactionTable.get(getTransNum(2)).lastLSN);
        assertEquals((long) LSNs.get(8), transactionTable.get(getTransNum(3)).lastLSN);

        /*
        for (int i = 1; i <= 3; i++) {
            if (transactionTable.get(getTransNum(i)) == null) { continue; }

            //System.out.println("Transaction number " + i + " touched pages:");

            for (Long touchedPage : transactionTable.get(getTransNum(i)).touchedPages) {
                //System.out.print(touchedPage + ", ");
            }
            //System.out.println("");
        }
        */

        // Touched pages
        assertEquals(new HashSet<>(Collections.singletonList(getPageNum(2))), transactionTable.get(getTransNum(2)).touchedPages);
        assertEquals(new HashSet<>(Arrays.asList(getPageNum(1), getPageNum(3))), transactionTable.get(getTransNum(3)).touchedPages);

        // DPT
        assertTrue(dirtyPageTable.containsKey(getPageNum(1)));
        assertFalse(dirtyPageTable.containsKey(getPageNum(2)));
        assertTrue(dirtyPageTable.containsKey(getPageNum(3)));
        assertTrue(dirtyPageTable.containsKey(getPageNum(4)));

        assertEquals((long) LSNs.get(3), (long) dirtyPageTable.get(getPageNum(1)));
        assertEquals((long) LSNs.get(0), (long) dirtyPageTable.get(getPageNum(3)));
        assertEquals((long) LSNs.get(9), (long) dirtyPageTable.get(getPageNum(4)));

        // status/cleanup
        assertEquals(Transaction.Status.COMPLETE, transaction1.getStatus());
        assertTrue(transaction1.cleanedUp);
        assertEquals(Transaction.Status.RECOVERY_ABORTING, transaction2.getStatus());
        assertFalse(transaction2.cleanedUp);
        assertEquals(Transaction.Status.RECOVERY_ABORTING, transaction3.getStatus());
        assertFalse(transaction2.cleanedUp);

        // transaction counter - from begin checkpoint
        assertEquals(9876543210L, getTransactionCounter(recoveryManager));

        // FlushedLSN
        assertEquals(LogManagerImpl.maxLSN(LogManagerImpl.getLSNPage(LSNs.get(11))), logManager.getFlushedLSN());
    }

    @Test
    public void testStudentRedo() throws Exception {
        // TODO(hw5): write your own test on restartRedo only
        // You should use loadRecoveryManager instead of new ARIESRecoveryManager(..) to
        // create the recovery manager, and use runRedo(inner) instead of
        // inner.restartRedo() to call the analysis routine.

        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        LogManager logManager = getLogManager(recoveryManager);
        DiskSpaceManager dsm = getDiskSpaceManager(recoveryManager);
        BufferManager bm = getBufferManager(recoveryManager);

        DummyTransaction transaction1 = DummyTransaction.create(1L);

        List<Long> LSNs = new ArrayList<>();
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(1L, 10000000001L, 0L, (short) 0, before,
                after))); // 0
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(1L, 10000000002L, LSNs.get(0), (short) 1,
                before, after))); // 1
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(1L, 10000000002L, LSNs.get(1), (short) 1,
                after, before))); // 2
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(1L, 10000000003L, LSNs.get(2), (short) 2,
                before, after))); // 3
        LSNs.add(logManager.appendToLog(new AllocPartLogRecord(1L, 10, LSNs.get(3)))); // 4
        LSNs.add(logManager.appendToLog(new CommitTransactionLogRecord(1L, LSNs.get(4)))); // 5
        LSNs.add(logManager.appendToLog(new EndTransactionLogRecord(1L, LSNs.get(5)))); // 6

        // actually do the first and second write (and get it flushed to disk)
        logManager.fetchLogRecord(LSNs.get(0)).redo(dsm, bm);
        logManager.fetchLogRecord(LSNs.get(1)).redo(dsm, bm);

        // flush everything - recovery tests should always start
        // with a clean load from disk, and here we want everything sent to disk first.
        // Note: this does not call RecoveryManager#close - it only closes the
        // buffer manager and disk space manager.
        shutdownRecoveryManager(recoveryManager);

        // load from disk again
        recoveryManager = loadRecoveryManager(testDir);

        // set up dirty page table - xact table is empty (transaction ended)
        Map<Long, Long> dirtyPageTable = getDirtyPageTable(recoveryManager);
        dirtyPageTable.put(10000000002L, LSNs.get(2));
        dirtyPageTable.put(10000000003L, LSNs.get(3));

        // set up checks for redo - these get called in sequence with each LogRecord#redo call
        setupRedoChecks(Arrays.asList(
                (LogRecord record) -> assertEquals((long) LSNs.get(2), (long) record.LSN),
                (LogRecord record) -> assertEquals((long) LSNs.get(3), (long) record.LSN),
                (LogRecord record) -> assertEquals((long) LSNs.get(4), (long) record.LSN)
        ));

        runRedo(recoveryManager);

        finishRedoChecks();
    }

    @Test
    public void testStudentUndo() throws Exception {
        // TODO(hw5): write your own test on restartUndo only
        // You should use loadRecoveryManager instead of new ARIESRecoveryManager(..) to
        // create the recovery manager, and use runUndo(inner) instead of
        // inner.restartUndo() to call the analysis routine.

        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        LogManager logManager = getLogManager(recoveryManager);
        DiskSpaceManager dsm = getDiskSpaceManager(recoveryManager);
        BufferManager bm = getBufferManager(recoveryManager);

        DummyTransaction transaction1 = DummyTransaction.create(1L);

        List<Long> LSNs = new ArrayList<>();
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(1L, 10000000001L, 0L, (short) 0, before,
                after))); // 0
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(1L, 10000000002L, LSNs.get(0), (short) 1,
                before, after))); // 1
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(1L, 10000000003L, LSNs.get(1), (short) 2,
                before, after))); // 2
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(1L, 10000000004L, LSNs.get(2), (short) 3,
                before, after))); // 3
        LSNs.add(logManager.appendToLog(new AbortTransactionLogRecord(1L, LSNs.get(3)))); // 4

        // actually do the writes
        for (int i = 0; i < 4; ++i) {
            logManager.fetchLogRecord(LSNs.get(i)).redo(dsm, bm);
        }

        // flush everything - recovery tests should always start
        // with a clean load from disk, and here we want everything sent to disk first.
        // Note: this does not call RecoveryManager#close - it only closes the
        // buffer manager and disk space manager.
        shutdownRecoveryManager(recoveryManager);

        // load from disk again
        recoveryManager = loadRecoveryManager(testDir);

        // set up xact table - leaving DPT empty
        Map<Long, TransactionTableEntry> transactionTable = getTransactionTable(recoveryManager);
        TransactionTableEntry entry1 = new TransactionTableEntry(transaction1);
        entry1.lastLSN = LSNs.get(4);
        entry1.touchedPages = new HashSet<>(Arrays.asList(10000000001L, 10000000002L, 10000000003L,
                10000000004L));
        entry1.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
        transactionTable.put(1L, entry1);

        // set up checks for undo - these get called in sequence with each LogRecord#redo call
        // (which should be called on CLRs)
        setupRedoChecks(Arrays.asList(
                (LogRecord record) -> {
                    assertEquals(LogType.UNDO_UPDATE_PAGE, record.getType());
                    assertNotNull("log record not appended to log yet", record.LSN);
                    assertEquals((long) record.LSN, transactionTable.get(1L).lastLSN);
                    assertEquals(Optional.of(10000000004L), record.getPageNum());
                },
                (LogRecord record) -> {
                    assertEquals(LogType.UNDO_UPDATE_PAGE, record.getType());
                    assertNotNull("log record not appended to log yet", record.LSN);
                    assertEquals((long) record.LSN, transactionTable.get(1L).lastLSN);
                    assertEquals(Optional.of(10000000003L), record.getPageNum());
                },
                (LogRecord record) -> {
                    assertEquals(LogType.UNDO_UPDATE_PAGE, record.getType());
                    assertNotNull("log record not appended to log yet", record.LSN);
                    assertEquals((long) record.LSN, transactionTable.get(1L).lastLSN);
                    assertEquals(Optional.of(10000000002L), record.getPageNum());
                },
                (LogRecord record) -> {
                    assertEquals(LogType.UNDO_UPDATE_PAGE, record.getType());
                    assertNotNull("log record not appended to log yet", record.LSN);
                    assertEquals((long) record.LSN, transactionTable.get(1L).lastLSN);
                    assertEquals(Optional.of(10000000001L), record.getPageNum());
                }
        ));

        runUndo(recoveryManager);

        finishRedoChecks();

        assertEquals(Transaction.Status.COMPLETE, transaction1.getStatus());
    }

    @Test
    public void testStudentIntegration() throws Exception {
        // TODO(hw5): write your own test on all of RecoveryManager
        // You should use loadRecoveryManager instead of new ARIESRecoveryManager(..) to
        // create the recovery manager.

        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF };

        Transaction transaction1 = DummyTransaction.create(1L);
        recoveryManager.startTransaction(transaction1);
        Transaction transaction2 = DummyTransaction.create(2L);
        recoveryManager.startTransaction(transaction2);
        Transaction transaction3 = DummyTransaction.create(3L);
        recoveryManager.startTransaction(transaction3);

        long[] LSNs = new long[] {
                recoveryManager.logPageWrite(1L, 10000000001L, (short) 0, before, after), // 0
                recoveryManager.logPageWrite(2L, 10000000003L, (short) 0, before, after), // 1
                recoveryManager.commit(1L), // 2
                recoveryManager.logPageWrite(3L, 10000000004L, (short) 0, before, after), // 3
                recoveryManager.logPageWrite(2L, 10000000001L, (short) 0, after, before), // 4
                recoveryManager.end(1L), // 5
                recoveryManager.logPageWrite(3L, 10000000002L, (short) 0, before, after), // 6
                recoveryManager.abort(2), // 7
        };

        // flush everything - recovery tests should always start
        // with a clean load from disk, and here we want everything sent to disk first.
        // Note: this does not call RecoveryManager#close - it only closes the
        // buffer manager and disk space manager.
        shutdownRecoveryManager(recoveryManager);

        // load from disk again
        recoveryManager = loadRecoveryManager(testDir);

        LogManager logManager = getLogManager(recoveryManager);

        recoveryManager.restart().run(); // run everything in restart recovery

        Iterator<LogRecord> iter = logManager.iterator();
        assertEquals(LogType.MASTER, iter.next().getType());
        assertEquals(LogType.BEGIN_CHECKPOINT, iter.next().getType());
        assertEquals(LogType.END_CHECKPOINT, iter.next().getType());
        assertEquals(LogType.UPDATE_PAGE, iter.next().getType());
        assertEquals(LogType.UPDATE_PAGE, iter.next().getType());
        assertEquals(LogType.COMMIT_TRANSACTION, iter.next().getType());
        assertEquals(LogType.UPDATE_PAGE, iter.next().getType());
        assertEquals(LogType.UPDATE_PAGE, iter.next().getType());
        assertEquals(LogType.END_TRANSACTION, iter.next().getType());
        assertEquals(LogType.UPDATE_PAGE, iter.next().getType());
        assertEquals(LogType.ABORT_TRANSACTION, iter.next().getType());

        LogRecord record = iter.next();
        assertEquals(LogType.ABORT_TRANSACTION, record.getType());
        long LSN8 = record.LSN;

        record = iter.next();
        assertEquals(LogType.UNDO_UPDATE_PAGE, record.getType());
        assertEquals(LSN8, (long) record.getPrevLSN().orElseThrow(NoSuchElementException::new));
        assertEquals(LSNs[3], (long) record.getUndoNextLSN().orElseThrow(NoSuchElementException::new));
        long LSN9 = record.LSN;

        record = iter.next();
        assertEquals(LogType.UNDO_UPDATE_PAGE, record.getType());
        assertEquals(LSNs[7], (long) record.getPrevLSN().orElseThrow(NoSuchElementException::new));
        assertEquals(LSNs[1], (long) record.getUndoNextLSN().orElseThrow(NoSuchElementException::new));
        long LSN10 = record.LSN;

        record = iter.next();
        assertEquals(LogType.UNDO_UPDATE_PAGE, record.getType());
        assertEquals(LSN9, (long) record.getPrevLSN().orElseThrow(NoSuchElementException::new));
        assertEquals(0L, (long) record.getUndoNextLSN().orElseThrow(NoSuchElementException::new));
        assertEquals(LogType.END_TRANSACTION, iter.next().getType());

        record = iter.next();
        assertEquals(LogType.UNDO_UPDATE_PAGE, record.getType());
        assertEquals(LSN10, (long) record.getPrevLSN().orElseThrow(NoSuchElementException::new));
        assertEquals(0L, (long) record.getUndoNextLSN().orElseThrow(NoSuchElementException::new));

        assertEquals(LogType.END_TRANSACTION, iter.next().getType());
        assertEquals(LogType.BEGIN_CHECKPOINT, iter.next().getType());
        assertEquals(LogType.END_CHECKPOINT, iter.next().getType());
        assertFalse(iter.hasNext());
    }

    // TODO(hw5): add as many (ungraded) tests as you want for testing!

    @Test
    public void testCase() throws Exception {
        // TODO(hw5): write your own test! (ungraded)
    }

    @Test
    public void anotherTestCase() throws Exception {
        // TODO(hw5): write your own test!!! (ungraded)
    }

    @Test
    public void yetAnotherTestCase() throws Exception {
        // TODO(hw5): write your own test!!!!! (ungraded)
    }

    /*************************************************************************
     * Helpers for writing tests.                                            *
     * Do not change the signature of any of the following methods.          *
     *************************************************************************/

    private void createEndCheckpointDataRecords(LogManager logManager,
                                                Map<Long, Long> dirtyPageTable,
                                                Map<Long, TransactionTableEntry> transactionTable,
                                                long beginLSN,
                                                List<Long> LSNs) {

        Map<Long, Long> dpt = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> txnTable = new HashMap<>();
        Map<Long, List<Long>> touchedPages = new HashMap<>();
        int numTouchedPages = 0;

        // Iterate through dirty page table pageNums and recLSNs
        for (Map.Entry<Long, Long> entry : dirtyPageTable.entrySet()) {
            long pageNum = entry.getKey();
            long recLSN = entry.getValue();

            boolean fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                    dpt.size(), txnTable.size(), touchedPages.size(), 0);

            if (!fitsAfterAdd) {
                LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                LSNs.add(logManager.appendToLog(endRecord));

                dpt.clear();
                txnTable.clear();
                touchedPages.clear();
            }

            dpt.put(pageNum, recLSN);
        }

        // Iterate through transaction statuses and LSNs
        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            long transNum = entry.getKey();
            TransactionTableEntry transactionTableEntry = entry.getValue();

            boolean fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                    dpt.size(), txnTable.size(), touchedPages.size(), 0);

            if (!fitsAfterAdd) {
                LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                LSNs.add(logManager.appendToLog(endRecord));

                dpt.clear();
                txnTable.clear();
                touchedPages.clear();
            }

            txnTable.put(transNum, new Pair<>(transactionTableEntry.transaction.getStatus(), transactionTableEntry.lastLSN));
        }

        // Iterate through touched pages
        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            long transNum = entry.getKey();
            for (long pageNum : entry.getValue().touchedPages) {
                boolean fitsAfterAdd;
                if (!touchedPages.containsKey(transNum)) {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                            dpt.size(), txnTable.size(), touchedPages.size() + 1, numTouchedPages + 1);
                } else {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                            dpt.size(), txnTable.size(), touchedPages.size(), numTouchedPages + 1);
                }

                if (!fitsAfterAdd) {
                    LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                    LSNs.add(logManager.appendToLog(endRecord));

                    dpt.clear();
                    txnTable.clear();
                    touchedPages.clear();
                    numTouchedPages = 0;
                }

                touchedPages.computeIfAbsent(transNum, t -> new ArrayList<>());
                touchedPages.get(transNum).add(pageNum);
                ++numTouchedPages;
            }
        }

        // Last end checkpoint record
        LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
        LSNs.add(logManager.appendToLog(endRecord));

        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    /**
     * Helper to get the logical transaction number from sequentially designated transaction numbers.
     */
    private long getTransNum(int i) {
        return (long) i;
    }

    /**
     * Helper to get the logical page number from sequentially designated page numbers.
     */
    private long getPageNum(int i) {
        return 10000000000L + (long) i;
    }

    /**
     * Helper to get the informal page number from the logical page number.
     */
    private int getInformalPageNum(long pageNum) {
        return (int) (pageNum - 10000000000L);
    }

    /**
     * Helper to set up checks for redo. The first call to LogRecord.redo will
     * call the first method in METHODS, the second call to the second method in METHODS,
     * and so on. Call this method before the redo pass, and call finishRedoChecks
     * after the redo pass.
     */
    private void setupRedoChecks(Collection<Consumer<LogRecord>> methods) {
        for (final Consumer<LogRecord> method : methods) {
            redoMethods.add(record -> {
                method.accept(record);
                LogRecord.onRedoHandler(redoMethods.poll());
            });
        }
        redoMethods.add(record -> {
            fail("LogRecord#redo() called too many times");
        });
        LogRecord.onRedoHandler(redoMethods.poll());
    }

    /**
     * Helper to finish checks for redo. Call this after the redo pass (or undo pass)-
     * if not enough redo calls were performed, an error is thrown.
     *
     * If setupRedoChecks is used for the redo pass, and this method is not called before
     * the undo pass, and the undo pass calls undo at least once, an error may be incorrectly thrown.
     */
    private void finishRedoChecks() {
        assertTrue("LogRecord#redo() not called enough times", redoMethods.isEmpty());
        LogRecord.onRedoHandler(record -> {});
    }

    /**
     * Loads the recovery manager from disk.
     * @param dir testDir
     * @return recovery manager, loaded from disk
     */
    protected RecoveryManager loadRecoveryManager(String dir) throws Exception {
        RecoveryManager recoveryManager = new ARIESRecoveryManagerNoLocking(
            new DummyLockContext(new Pair<>("database", 0L)),
            DummyTransaction::create
        );
        DiskSpaceManager diskSpaceManager = new DiskSpaceManagerImpl(dir, recoveryManager);
        BufferManager bufferManager = new BufferManagerImpl(diskSpaceManager, recoveryManager, 32,
                new LRUEvictionPolicy());
        boolean isLoaded = true;
        try {
            diskSpaceManager.allocPart(0);
            diskSpaceManager.allocPart(1);
            for (int i = 0; i < 10; ++i) {
                diskSpaceManager.allocPage(DiskSpaceManager.getVirtualPageNum(1, i));
            }
            isLoaded = false;
        } catch (IllegalStateException e) {
            // already loaded
        }
        recoveryManager.setManagers(diskSpaceManager, bufferManager);
        if (!isLoaded) {
            recoveryManager.initialize();
        }
        return recoveryManager;
    }

    /**
     * Flushes everything to disk, but does not call RecoveryManager#shutdown. Similar
     * to pulling the plug on the database at a time when no changes are in memory. You
     * can simulate a shutdown where certain changes _are_ in memory, by simply never
     * applying them (i.e. write a log record, but do not make the changes on the
     * buffer manager/disk space manager).
     */
    protected void shutdownRecoveryManager(RecoveryManager recoveryManager) throws Exception {
        ARIESRecoveryManager arm = (ARIESRecoveryManager) recoveryManager;
        arm.logManager.close();
        arm.bufferManager.evictAll();
        arm.bufferManager.close();
        arm.diskSpaceManager.close();
        DummyTransaction.cleanupTransactions();
    }

    protected BufferManager getBufferManager(RecoveryManager recoveryManager) throws Exception {
        return ((ARIESRecoveryManager) recoveryManager).bufferManager;
    }

    protected DiskSpaceManager getDiskSpaceManager(RecoveryManager recoveryManager) throws Exception {
        return ((ARIESRecoveryManager) recoveryManager).diskSpaceManager;
    }

    protected LogManager getLogManager(RecoveryManager recoveryManager) throws Exception {
        return ((ARIESRecoveryManager) recoveryManager).logManager;
    }

    protected List<String> getLockRequests(RecoveryManager recoveryManager) throws Exception {
        return ((ARIESRecoveryManager) recoveryManager).lockRequests;
    }

    protected long getTransactionCounter(RecoveryManager recoveryManager) throws Exception {
        return ((ARIESRecoveryManagerNoLocking) recoveryManager).transactionCounter;
    }

    protected Map<Long, Long> getDirtyPageTable(RecoveryManager recoveryManager) throws Exception {
        return ((ARIESRecoveryManager) recoveryManager).dirtyPageTable;
    }

    protected Map<Long, TransactionTableEntry> getTransactionTable(RecoveryManager recoveryManager)
    throws Exception {
        return ((ARIESRecoveryManager) recoveryManager).transactionTable;
    }

    protected void runAnalysis(RecoveryManager recoveryManager) throws Exception {
        ((ARIESRecoveryManager) recoveryManager).restartAnalysis();
    }

    protected void runRedo(RecoveryManager recoveryManager) throws Exception {
        ((ARIESRecoveryManager) recoveryManager).restartRedo();
    }

    protected void runUndo(RecoveryManager recoveryManager) throws Exception {
        ((ARIESRecoveryManager) recoveryManager).restartUndo();
    }
}
