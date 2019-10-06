package edu.berkeley.cs186.database;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.UnaryOperator;

import edu.berkeley.cs186.database.common.ByteBuffer;
import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.*;
import edu.berkeley.cs186.database.databox.*;
import edu.berkeley.cs186.database.index.BPlusTree;
import edu.berkeley.cs186.database.index.BPlusTreeMetadata;
import edu.berkeley.cs186.database.io.*;
import edu.berkeley.cs186.database.memory.*;
import edu.berkeley.cs186.database.query.QueryPlan;
import edu.berkeley.cs186.database.query.QueryPlanException;
import edu.berkeley.cs186.database.query.SortOperator;
import edu.berkeley.cs186.database.recovery.*;
import edu.berkeley.cs186.database.table.*;
import edu.berkeley.cs186.database.table.stats.TableStats;

@SuppressWarnings("unused")
public class Database implements AutoCloseable {
    private static final String METADATA_TABLE_PREFIX = "information_schema.";
    private static final String TABLE_INFO_TABLE_NAME = METADATA_TABLE_PREFIX + "tables";
    private static final String INDEX_INFO_TABLE_NAME = METADATA_TABLE_PREFIX + "indices";
    private static final int DEFAULT_BUFFER_SIZE = 262144; // default of 1G
    private static final int MAX_SCHEMA_SIZE = 4001;

    // information_schema.tables, manages all tables in the database
    private Table tableInfo;
    // information_schema.indices, manages all indices in the database
    private Table indexInfo;
    // table name to table object mapping
    private final ConcurrentMap<String, Table> tableLookup;
    // index name to bplustree object mapping (index name is: "table,col")
    private final ConcurrentMap<String, BPlusTree> indexLookup;
    // table name to record id of entry in tableInfo
    private final ConcurrentMap<String, RecordId> tableInfoLookup;
    // index name to record id of entry in indexInfo
    private final ConcurrentMap<String, RecordId> indexInfoLookup;
    // list of indices for each table
    private final ConcurrentMap<String, List<String>> tableIndices;

    // number of transactions created
    private long numTransactions;

    // lock manager
    private final LockManager lockManager;
    // disk space manager
    private final DiskSpaceManager diskSpaceManager;
    // buffer manager
    private final BufferManager bufferManager;
    // recovery manager
    private final RecoveryManager recoveryManager;

    // transaction for setup/cleanup
    private final Transaction initTransaction;
    // transaction context for recovery manager; only used to hold locks
    private final TransactionContext recoveryTransaction;
    // thread pool for background tasks
    private final ExecutorService executor;

    // number of pages of memory to use for joins, etc.
    private int workMem = 1024; // default of 4M
    // number of pages of memory available total
    private int numMemoryPages;

    // progress in loading tables/indices
    private final Phaser loadingProgress = new Phaser(1);
    // active transactions
    private Phaser activeTransactions = new Phaser(0);

    /**
     * Creates a new database with locking disabled.
     *
     * @param fileDir the directory to put the table files in
     */
    public Database(String fileDir) {
        this (fileDir, DEFAULT_BUFFER_SIZE);
    }

    /**
     * Creates a new database with locking disabled.
     *
     * @param fileDir the directory to put the table files in
     * @param numMemoryPages the number of pages of memory in the buffer cache
     */
    public Database(String fileDir, int numMemoryPages) {
        this(fileDir, numMemoryPages, new DummyLockManager());
        waitSetupFinished();
    }

    /**
     * Creates a new database.
     *
     * @param fileDir the directory to put the table files in
     * @param numMemoryPages the number of pages of memory in the buffer cache
     * @param lockManager the lock manager
     */
    public Database(String fileDir, int numMemoryPages, LockManager lockManager) {
        this(fileDir, numMemoryPages, lockManager, new ClockEvictionPolicy());
    }

    /**
     * Creates a new database.
     *
     * @param fileDir the directory to put the table files in
     * @param numMemoryPages the number of pages of memory in the buffer cache
     * @param lockManager the lock manager
     * @param policy eviction policy for buffer cache
     */
    public Database(String fileDir, int numMemoryPages, LockManager lockManager,
                    EvictionPolicy policy) {
        boolean initialized = setupDirectory(fileDir);

        numTransactions = 0;
        this.numMemoryPages = numMemoryPages;
        this.lockManager = lockManager;
        tableLookup = new ConcurrentHashMap<>();
        indexLookup = new ConcurrentHashMap<>();
        tableIndices = new ConcurrentHashMap<>();
        tableInfoLookup = new ConcurrentHashMap<>();
        indexInfoLookup = new ConcurrentHashMap<>();
        this.executor = new ThreadPool();

        recoveryTransaction = new TransactionContextImpl(-1);
        initTransaction = beginTransaction();
        TransactionContext.setTransaction(initTransaction.getTransactionContext());

        // TODO(hw5): change to use ARIES recovery manager
        recoveryManager = new DummyRecoveryManager();
        // recoveryManager = new ARIESRecoveryManager(lockManager.databaseContext(), getLogContext(),
        //                                            this::beginTransaction, recoveryTransaction);

        diskSpaceManager = new DiskSpaceManagerImpl(fileDir, recoveryManager);
        bufferManager = new BufferManagerImpl(diskSpaceManager, recoveryManager, numMemoryPages,
                                              policy);

        recoveryManager.setManagers(diskSpaceManager, bufferManager);

        if (!initialized) {
            // create log partition, information_schema.tables partition, and information_schema.indices partition
            diskSpaceManager.allocPart(0);
            diskSpaceManager.allocPart(1);
            diskSpaceManager.allocPart(2);

            recoveryManager.initialize();
        } else {
            Runnable r = recoveryManager.restart();
            executor.submit(r);
        }

        LockContext dbContext = lockManager.databaseContext();
        LockContext tableInfoContext = getTableInfoContext();

        if (!initialized) {
            dbContext.acquire(initTransaction.getTransactionContext(), LockType.X);
            this.initTableInfo();
            this.initIndexInfo();
            initTransaction.commit();
            this.loadingProgress.arriveAndDeregister();
        } else {
            dbContext.acquire(initTransaction.getTransactionContext(), LockType.IX);
            tableInfoContext.acquire(initTransaction.getTransactionContext(), LockType.S);
            this.loadTableInfo();
            this.loadTables();
        }

        TransactionContext.unsetTransaction();
    }

    private boolean setupDirectory(String fileDir) {
        File dir = new File(fileDir);
        boolean initialized = dir.exists();
        if (!initialized) {
            if (!dir.mkdir()) {
                throw new DatabaseException("failed to create directory " + fileDir);
            }
        } else if (!dir.isDirectory()) {
            throw new DatabaseException(fileDir + " is not a directory");
        }
        try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(dir.toPath())) {
            initialized = initialized && dirStream.iterator().hasNext();
        } catch (IOException e) {
            throw new DatabaseException(e);
        }
        return initialized;
    }

    // create information_schema.tables
    private void initTableInfo() {
        long tableInfoPage0 = DiskSpaceManager.getVirtualPageNum(1, 0);
        diskSpaceManager.allocPage(tableInfoPage0);

        LockContext tableInfoContext = getTableInfoContext();
        HeapFile tableInfoHeapFile = new PageDirectory(bufferManager, 1, tableInfoPage0, (short) 0,
                tableInfoContext);
        tableInfo = new Table(TABLE_INFO_TABLE_NAME, getTableInfoSchema(), tableInfoHeapFile,
                              tableInfoContext);
        tableInfoLookup.put(TABLE_INFO_TABLE_NAME, tableInfo.addRecord(Arrays.asList(
                                new StringDataBox(TABLE_INFO_TABLE_NAME, 32),
                                new IntDataBox(1),
                                new LongDataBox(tableInfoPage0),
                                new BoolDataBox(false),
                                new IntDataBox(0),
                                new StringDataBox(new String(getTableInfoSchema().toBytes()), MAX_SCHEMA_SIZE)
                            )));
        tableLookup.put(TABLE_INFO_TABLE_NAME, tableInfo);
        tableIndices.put(TABLE_INFO_TABLE_NAME, Collections.emptyList());
    }

    // create information_schema.indices
    private void initIndexInfo() {
        long indexInfoPage0 = DiskSpaceManager.getVirtualPageNum(2, 0);
        diskSpaceManager.allocPage(indexInfoPage0);

        LockContext indexInfoContext = getIndexInfoContext();
        HeapFile heapFile = new PageDirectory(bufferManager, 2, indexInfoPage0, (short) 0,
                                              indexInfoContext);
        indexInfo = new Table(INDEX_INFO_TABLE_NAME, getIndexInfoSchema(), heapFile, indexInfoContext);
        indexInfo.setFullPageRecords();
        tableInfoLookup.put(INDEX_INFO_TABLE_NAME, tableInfo.addRecord(Arrays.asList(
                                new StringDataBox(INDEX_INFO_TABLE_NAME, 32),
                                new IntDataBox(2),
                                new LongDataBox(indexInfoPage0),
                                new BoolDataBox(false),
                                new IntDataBox(0),
                                new StringDataBox(new String(getIndexInfoSchema().toBytes()), MAX_SCHEMA_SIZE)
                            )));
        tableLookup.put(INDEX_INFO_TABLE_NAME, indexInfo);
        tableIndices.put(INDEX_INFO_TABLE_NAME, Collections.emptyList());
    }

    private void loadTableInfo() {
        LockContext tableInfoContext = getTableInfoContext();
        // load information_schema.tables
        HeapFile tableInfoHeapFile = new PageDirectory(bufferManager, 1,
                DiskSpaceManager.getVirtualPageNum(1, 0), (short) 0, tableInfoContext);
        tableInfo = new Table(TABLE_INFO_TABLE_NAME, getTableInfoSchema(), tableInfoHeapFile,
                              tableInfoContext);
        tableLookup.put(TABLE_INFO_TABLE_NAME, tableInfo);
        tableIndices.put(TABLE_INFO_TABLE_NAME, Collections.emptyList());
    }

    // load tables from information_schema.tables
    private void loadTables() {
        Map<String, CountDownLatch> tableIndexCount = new ConcurrentHashMap<>();
        for (RecordId recordId : (Iterable<RecordId>) tableInfo::ridIterator) {
            TableInfoRecord record = new TableInfoRecord(tableInfo.getRecord(recordId));
            if (!record.isAllocated()) {
                tableInfo.deleteRecord(recordId);
                continue;
            }
            tableInfoLookup.put(record.tableName, recordId);
            tableIndexCount.put(record.tableName, new CountDownLatch(record.numIndices));

            if (record.tableName.equals(TABLE_INFO_TABLE_NAME)) {
                continue;
            }

            LockContext tableContext = getTableContext(record.tableName, record.partNum);
            if (tableContext.getExplicitLockType(initTransaction.getTransactionContext()) == LockType.NL) {
                tableContext.acquire(initTransaction.getTransactionContext(), LockType.X);
            }

            loadingProgress.register();
            executor.execute(() -> {
                loadingProgress.arriveAndAwaitAdvance();

                HeapFile heapFile = new PageDirectory(bufferManager, record.partNum, record.pageNum, (short) 0,
                                                      tableContext);
                Table table = new Table(record.tableName, record.schema, heapFile, tableContext);
                if (record.tableName.equals(INDEX_INFO_TABLE_NAME)) {
                    indexInfo = table;
                    indexInfo.setFullPageRecords();
                    this.loadIndices(tableIndexCount);
                }

                if (!record.isTemporary) {
                    tableLookup.put(record.tableName, table);
                    // the list only needs to be synchronized while indices are being loaded, as multiple
                    // indices may attempt to add themselves to the list at the same time
                    tableIndices.putIfAbsent(record.tableName, Collections.synchronizedList(new ArrayList<>()));
                }

                CountDownLatch latch = tableIndexCount.get(record.tableName);
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw new DatabaseException(e);
                }
                synchronized (initTransaction) {
                    tableContext.release(initTransaction.getTransactionContext());
                }

                loadingProgress.arriveAndDeregister();
            });
        }

        loadingProgress.arriveAndAwaitAdvance();

        executor.execute(() -> {
            loadingProgress.arriveAndAwaitAdvance();
            initTransaction.commit();
            loadingProgress.arriveAndDeregister();
        });
    }

    // load indices from information_schema.indices
    private void loadIndices(Map<String, CountDownLatch> tableIndexCount) {
        for (RecordId recordId : (Iterable<RecordId>) indexInfo::ridIterator) {
            loadingProgress.register();
            executor.execute(() -> {
                try {
                    BPlusTreeMetadata metadata = getIndexMetadata(indexInfo.getRecord(recordId));
                    if (metadata == null) {
                        indexInfo.deleteRecord(recordId);
                        return;
                    }
                    String indexName = metadata.getName();
                    LockContext indexContext = getIndexContext(indexName, metadata.getPartNum());
                    synchronized (initTransaction) {
                        indexContext.acquire(initTransaction.getTransactionContext(), LockType.X);
                    }

                    BPlusTree tree = new BPlusTree(bufferManager, metadata, indexContext);
                    if (!tableIndices.containsKey(metadata.getTableName())) {
                        // the list only needs to be synchronized while indices are being loaded, as multiple
                        // indices may attempt to add themselves to the list at the same time
                        tableIndices.put(metadata.getTableName(), Collections.synchronizedList(new ArrayList<>()));
                    }
                    tableIndices.get(metadata.getTableName()).add(indexName);
                    indexLookup.put(indexName, tree);
                    indexInfoLookup.put(indexName, recordId);

                    synchronized (initTransaction) {
                        indexContext.release(initTransaction.getTransactionContext());
                    }
                    tableIndexCount.get(prefixUserTableName(metadata.getTableName())).countDown();
                } finally {
                    loadingProgress.arriveAndDeregister();
                }
            });
        }
    }

    // wait until setup has finished
    public void waitSetupFinished() {
        while (!loadingProgress.isTerminated()) {
            loadingProgress.awaitAdvance(loadingProgress.getPhase());
        }
    }

    // wait for all transactions to finish
    public synchronized void waitAllTransactions() {
        while (!activeTransactions.isTerminated()) {
            activeTransactions.awaitAdvance(activeTransactions.getPhase());
        }
    }

    /**
     * Close this database.
     */
    @Override
    public synchronized void close() {
        if (this.executor.isShutdown()) {
            return;
        }

        // wait for all transactions to terminate
        this.waitAllTransactions();

        // clear out placeholder tableInfo entries
        for (RecordId recordId : (Iterable<RecordId>) tableInfo::ridIterator) {
            if (!new TableInfoRecord(tableInfo.getRecord(recordId)).isAllocated()) {
                tableInfo.deleteRecord(recordId);
            }
        }

        // clear out placeholder indexInfo entries
        for (RecordId recordId : (Iterable<RecordId>) indexInfo::ridIterator) {
            if (getIndexMetadata(indexInfo.getRecord(recordId)) == null) {
                indexInfo.deleteRecord(recordId);
            }
        }

        this.bufferManager.evictAll();

        this.recoveryManager.close();
        this.recoveryTransaction.close();

        this.tableInfo = null;
        this.indexInfo = null;

        this.tableLookup.clear();
        this.indexLookup.clear();
        this.tableInfoLookup.clear();
        this.indexInfoLookup.clear();
        this.tableIndices.clear();

        this.bufferManager.close();
        this.diskSpaceManager.close();

        this.executor.shutdown();
    }

    public ExecutorService getExecutor() {
        return executor;
    }

    public LockManager getLockManager() {
        return lockManager;
    }

    public DiskSpaceManager getDiskSpaceManager() {
        return diskSpaceManager;
    }

    public BufferManager getBufferManager() {
        return bufferManager;
    }

    public Table getTable(String tableName) {
        return tableLookup.get(prefixUserTableName(tableName));
    }

    public int getWorkMem() {
        // cap work memory at number of memory pages -- this is likely to cause out of memory
        // errors if actually set this high
        return this.workMem > this.numMemoryPages ? this.numMemoryPages : this.workMem;
    }

    public void setWorkMem(int workMem) {
        this.workMem = workMem;
    }

    // schema for information_schema.tables
    private Schema getTableInfoSchema() {
        return new Schema(
                   Arrays.asList("table_name", "part_num", "page_num", "is_temporary", "num_indices", "schema"),
                   Arrays.asList(Type.stringType(32), Type.intType(), Type.longType(), Type.boolType(),
                                 Type.intType(), Type.stringType(MAX_SCHEMA_SIZE))
               );
    }

    // schema for information_schema.indices
    private Schema getIndexInfoSchema() {
        return new Schema(
                   Arrays.asList("table_name", "col_name", "order", "part_num", "root_page_num", "key_schema_typeid",
                                 "key_schema_typesize", "height"),
                   Arrays.asList(Type.stringType(32), Type.stringType(32), Type.intType(), Type.intType(),
                                 Type.longType(), Type.intType(), Type.intType(), Type.intType())
               );
    }

    // a single row of information_schema.tables
    private static class TableInfoRecord {
        String tableName;
        int partNum;
        long pageNum;
        boolean isTemporary;
        int numIndices;
        Schema schema;

        TableInfoRecord(String tableName) {
            this.tableName = tableName;
            this.partNum = -1;
            this.pageNum = -1;
            this.isTemporary = false;
            this.numIndices = 0;
            this.schema = new Schema(Collections.emptyList(), Collections.emptyList());
        }

        TableInfoRecord(Record record) {
            List<DataBox> values = record.getValues();
            tableName = values.get(0).getString();
            partNum = values.get(1).getInt();
            pageNum = values.get(2).getLong();
            isTemporary = values.get(3).getBool();
            numIndices = values.get(4).getInt();
            schema = Schema.fromBytes(ByteBuffer.wrap(values.get(5).toBytes()));
        }

        List<DataBox> toDataBox() {
            return Arrays.asList(
                       new StringDataBox(tableName, 32),
                       new IntDataBox(partNum),
                       new LongDataBox(pageNum),
                       new BoolDataBox(isTemporary),
                       new IntDataBox(numIndices),
                       new StringDataBox(new String(schema.toBytes()), MAX_SCHEMA_SIZE)
                   );
        }

        boolean isAllocated() {
            return this.partNum >= 0;
        }
    }

    // row of information_schema.indices --> BPlusTreeMetadata
    private BPlusTreeMetadata getIndexMetadata(Record record) {
        List<DataBox> values = record.getValues();
        String tableName = values.get(0).getString();
        String colName = values.get(1).getString();
        int order = values.get(2).getInt();
        int partNum = values.get(3).getInt();
        long rootPageNum = values.get(4).getLong();
        int height = values.get(7).getInt();

        if (partNum < 0) {
            return null;
        }

        Type keySchema = new Type(TypeId.values()[values.get(5).getInt()], values.get(6).getInt());
        return new BPlusTreeMetadata(tableName, colName, keySchema, order, partNum, rootPageNum, height);
    }

    // get the lock context for the log
    private LockContext getLogContext() {
        return lockManager.context("log", 1);
    }

    // get the lock context for information_schema.tables
    private LockContext getTableInfoContext() {
        return lockManager.databaseContext().childContext(TABLE_INFO_TABLE_NAME, 1L);
    }

    // get the lock context for information_schema.tables
    private LockContext getIndexInfoContext() {
        return lockManager.databaseContext().childContext(INDEX_INFO_TABLE_NAME, 2L);
    }

    // get the lock context for a table
    private LockContext getTableContext(String table, int partNum) {
        return lockManager.databaseContext().childContext(prefixUserTableName(table), partNum);
    }

    // get the lock context for a table
    private LockContext getTableContext(String table) {
        return getTableContext(table, tableLookup.get(prefixUserTableName(table)).getPartNum());
    }

    // get the lock context for an index
    LockContext getIndexContext(String index, int partNum) {
        return lockManager.databaseContext().childContext("indices." + index, partNum);
    }

    // get the lock context for an index
    LockContext getIndexContext(String index) {
        return getIndexContext(index, indexLookup.get(index).getPartNum());
    }

    private String prefixUserTableName(String table) {
        if (table.contains(".")) {
            return table;
        } else {
            return "tables." + table;
        }
    }

    private RecordId getTableInfoRecordId(String tableName) {
        return Database.this.tableInfoLookup.compute(tableName, (String name, RecordId rid) -> {
            if (rid == null) {
                return Database.this.tableInfo.addRecord(new TableInfoRecord(name).toDataBox());
            }
            return rid;
        });
    }

    private RecordId getIndexInfoRecordId(String tableName, String columnName) {
        String indexName = tableName + "," + columnName;
        return Database.this.indexInfoLookup.compute(indexName, (String name, RecordId rid) -> {
            if (rid == null) {
                return Database.this.indexInfo.addRecord(Arrays.asList(
                            new StringDataBox(tableName, 32),
                            new StringDataBox(columnName, 32),
                            new IntDataBox(-1),
                            new IntDataBox(-1),
                            new LongDataBox(DiskSpaceManager.INVALID_PAGE_NUM),
                            new IntDataBox(TypeId.INT.ordinal()),
                            new IntDataBox(4),
                            new IntDataBox(-1)
                        ));
            }
            return rid;
        });
    }

    /**
     * Start a new transaction.
     *
     * @return the new Transaction
     */
    public synchronized Transaction beginTransaction() {
        return beginTransaction(this.numTransactions);
    }

    /**
     * Start a new transaction.
     *
     * @return the new Transaction
     */
    private synchronized Transaction beginTransaction(Long transactionNum) {
        this.numTransactions = Math.max(this.numTransactions, transactionNum + 1);

        TransactionImpl t = new TransactionImpl(transactionNum);
        activeTransactions.register();
        if (activeTransactions.isTerminated()) {
            activeTransactions = new Phaser(1);
        }
        return t;
    }

    private class TransactionContextImpl extends AbstractTransactionContext {
        long transNum;
        Map<String, String> aliases;
        Map<String, Table> tempTables;
        long tempTableCounter;

        private TransactionContextImpl(long tNum) {
            this.transNum = tNum;
            this.aliases = new HashMap<>();
            this.tempTables = new HashMap<>();
            this.tempTableCounter = 0;
        }

        @Override
        public long getTransNum() {
            return transNum;
        }

        @Override
        public int getWorkMemSize() {
            return Database.this.getWorkMem();
        }

        @Override
        public String createTempTable(Schema schema) {
            String tempTableName = "tempTable" + tempTableCounter++;
            String tableName = prefixTempTableName(tempTableName);

            int partNum = diskSpaceManager.allocPart();
            long pageNum = diskSpaceManager.allocPage(partNum);
            RecordId recordId = tableInfo.addRecord(Arrays.asList(
                    new StringDataBox(tableName, 32),
                    new IntDataBox(partNum),
                    new LongDataBox(pageNum),
                    new BoolDataBox(true),
                    new IntDataBox(0),
                    new StringDataBox(new String(schema.toBytes()), MAX_SCHEMA_SIZE)));
            tableInfoLookup.put(tableName, recordId);

            LockContext lockContext = getTableContext(tableName, partNum);
            lockContext.disableChildLocks();
            HeapFile heapFile = new PageDirectory(bufferManager, partNum, pageNum, (short) 0, lockContext);
            tempTables.put(tempTableName, new Table(tableName, schema, heapFile, lockContext));
            tableLookup.put(tableName, tempTables.get(tempTableName));
            tableIndices.put(tableName, Collections.emptyList());

            return tempTableName;
        }

        private void deleteTempTable(String tempTableName) {
            if (!this.tempTables.containsKey(tempTableName)) {
                return;
            }

            String tableName = prefixTempTableName(tempTableName);
            RecordId recordId = tableInfoLookup.remove(tableName);
            Record record = tableInfo.deleteRecord(recordId);
            TableInfoRecord tableInfoRecord = new TableInfoRecord(record);
            bufferManager.freePart(tableInfoRecord.partNum);
            tempTables.remove(tempTableName);
            tableLookup.remove(tableName);
            tableIndices.remove(tableName);
        }

        private void deleteAllTempTables() {
            Set<String> keys = new HashSet<>(tempTables.keySet());

            for (String tableName : keys) {
                deleteTempTable(tableName);
            }
        }

        @Override
        public void setAliasMap(Map<String, String> aliasMap) {
            this.aliases = new HashMap<>(aliasMap);
        }

        @Override
        public void clearAliasMap() {
            this.aliases.clear();
        }

        @Override
        public boolean indexExists(String tableName, String columnName) {
            try {
                resolveIndexFromName(tableName, columnName);
            } catch (DatabaseException e) {
                return false;
            }
            return true;
        }

        @Override
        public void updateIndexMetadata(BPlusTreeMetadata metadata) {
            indexInfo.updateRecord(Arrays.asList(
                                       new StringDataBox(metadata.getTableName(), 32),
                                       new StringDataBox(metadata.getColName(), 32),
                                       new IntDataBox(metadata.getOrder()),
                                       new IntDataBox(metadata.getPartNum()),
                                       new LongDataBox(metadata.getRootPageNum()),
                                       new IntDataBox(metadata.getKeySchema().getTypeId().ordinal()),
                                       new IntDataBox(metadata.getKeySchema().getSizeInBytes()),
                                       new IntDataBox(metadata.getHeight())
                                   ), indexInfoLookup.get(metadata.getName()));
        }

        @Override
        public Iterator<Record> sortedScan(String tableName, String columnName) {
            // TODO(hw4_part2): scan locking

            Table tab = getTable(tableName);
            try {
                Pair<String, BPlusTree> index = resolveIndexFromName(tableName, columnName);
                return new RecordIterator(tab, index.getSecond().scanAll());
            } catch (DatabaseException e1) {
                int offset = getTable(tableName).getSchema().getFieldNames().indexOf(columnName);
                try {
                    return new SortOperator(this, tableName,
                                            Comparator.comparing((Record r) -> r.getValues().get(offset))).iterator();
                } catch (QueryPlanException e2) {
                    throw new DatabaseException(e2);
                }
            }
        }

        @Override
        public Iterator<Record> sortedScanFrom(String tableName, String columnName, DataBox startValue) {
            // TODO(hw4_part2): scan locking

            Table tab = getTable(tableName);
            Pair<String, BPlusTree> index = resolveIndexFromName(tableName, columnName);
            return new RecordIterator(tab, index.getSecond().scanGreaterEqual(startValue));
        }

        @Override
        public Iterator<Record> lookupKey(String tableName, String columnName, DataBox key) {
            Table tab = getTable(tableName);
            Pair<String, BPlusTree> index = resolveIndexFromName(tableName, columnName);
            return new RecordIterator(tab, index.getSecond().scanEqual(key));
        }

        @Override
        public BacktrackingIterator<Record> getRecordIterator(String tableName) {
            return getTable(tableName).iterator();
        }

        @Override
        public BacktrackingIterator<Page> getPageIterator(String tableName) {
            return getTable(tableName).pageIterator();
        }

        @Override
        public BacktrackingIterator<Record> getBlockIterator(String tableName, Iterator<Page> block,
                int maxPages) {
            return getTable(tableName).blockIterator(block, maxPages);
        }

        @Override
        public boolean contains(String tableName, String columnName, DataBox key) {
            Pair<String, BPlusTree> index = resolveIndexFromName(tableName, columnName);
            return index.getSecond().get(key).isPresent();
        }

        @Override
        public RecordId addRecord(String tableName, List<DataBox> values) {
            Table tab = getTable(tableName);
            RecordId rid = tab.addRecord(values);
            Schema s = tab.getSchema();
            List<String> colNames = s.getFieldNames();

            for (String indexName : tableIndices.get(tab.getName())) {
                String column = indexName.split(",")[1];
                resolveIndexFromName(tableName, column).getSecond().put(values.get(colNames.indexOf(column)), rid);
            }
            return rid;
        }

        @Override
        public RecordId deleteRecord(String tableName, RecordId rid) {
            Table tab = getTable(tableName);
            Schema s = tab.getSchema();

            Record rec = tab.deleteRecord(rid);
            List<DataBox> values = rec.getValues();
            List<String> colNames = s.getFieldNames();

            for (String indexName : tableIndices.get(tab.getName())) {
                String column = indexName.split(",")[1];
                resolveIndexFromName(tableName, column).getSecond().remove(values.get(colNames.indexOf(column)));
            }
            return rid;
        }

        @Override
        public Record getRecord(String tableName, RecordId rid) {
            return getTable(tableName).getRecord(rid);
        }

        @Override
        public RecordId updateRecord(String tableName, List<DataBox> values, RecordId rid) {
            Table tab = getTable(tableName);
            Schema s = tab.getSchema();

            Record rec = tab.updateRecord(values, rid);

            List<DataBox> oldValues = rec.getValues();
            List<String> colNames = s.getFieldNames();

            for (String indexName : tableIndices.get(tab.getName())) {
                String column = indexName.split(",")[1];
                int i = colNames.indexOf(column);
                BPlusTree tree = resolveIndexFromName(tableName, column).getSecond();
                tree.remove(oldValues.get(i));
                tree.put(values.get(i), rid);
            }
            return rid;
        }

        @Override
        public void runUpdateRecordWhere(String tableName, String targetColumnName,
                                         UnaryOperator<DataBox> targetValue,
                                         String predColumnName, PredicateOperator predOperator, DataBox predValue) {
            Table tab = getTable(tableName);
            Iterator<RecordId> recordIds = tab.ridIterator();

            Schema s = tab.getSchema();
            int uindex = s.getFieldNames().indexOf(targetColumnName);
            int pindex = s.getFieldNames().indexOf(predColumnName);

            while(recordIds.hasNext()) {
                RecordId curRID = recordIds.next();
                Record cur = getRecord(tableName, curRID);
                List<DataBox> recordCopy = new ArrayList<>(cur.getValues());

                if (predOperator == null || predOperator.evaluate(recordCopy.get(pindex), predValue)) {
                    recordCopy.set(uindex, targetValue.apply(recordCopy.get(uindex)));
                    updateRecord(tableName, recordCopy, curRID);
                }
            }
        }

        @Override
        public void runDeleteRecordWhere(String tableName, String predColumnName,
                                         PredicateOperator predOperator, DataBox predValue) {
            Table tab = getTable(tableName);
            Iterator<RecordId> recordIds = tab.ridIterator();

            Schema s = tab.getSchema();
            int pindex = s.getFieldNames().indexOf(predColumnName);

            while(recordIds.hasNext()) {
                RecordId curRID = recordIds.next();
                Record cur = getRecord(tableName, curRID);
                List<DataBox> recordCopy = new ArrayList<>(cur.getValues());

                if (predOperator == null || predOperator.evaluate(recordCopy.get(pindex), predValue)) {
                    deleteRecord(tableName, curRID);
                }
            }
        }

        @Override
        public Schema getSchema(String tableName) {
            return getTable(tableName).getSchema();
        }

        @Override
        public Schema getFullyQualifiedSchema(String tableName) {
            Schema schema = getTable(tableName).getSchema();
            List<String> newColumnNames = new ArrayList<>();
            for (String oldName : schema.getFieldNames()) {
                newColumnNames.add(tableName + "." + oldName);
            }

            return new Schema(newColumnNames, schema.getFieldTypes());
        }

        @Override
        public TableStats getStats(String tableName) {
            return getTable(tableName).getStats();
        }

        @Override
        public int getNumDataPages(String tableName) {
            return getTable(tableName).getNumDataPages();
        }

        @Override
        public int getNumEntriesPerPage(String tableName) {
            return getTable(tableName).getNumRecordsPerPage();
        }

        @Override
        public int getEntrySize(String tableName) {
            return getTable(tableName).getSchema().getSizeInBytes();
        }

        @Override
        public long getNumRecords(String tableName) {
            return getTable(tableName).getNumRecords();
        }

        @Override
        public int getTreeOrder(String tableName, String columnName) {
            return resolveIndexMetadataFromName(tableName, columnName).getSecond().getOrder();
        }

        @Override
        public int getTreeHeight(String tableName, String columnName) {
            return resolveIndexMetadataFromName(tableName, columnName).getSecond().getHeight();
        }

        @Override
        public void close() {
            this.deleteAllTempTables();

            // TODO(hw4_part2): release all locks
        }

        @Override
        public String toString() {
            return "Transaction Context for Transaction " + transNum;
        }

        private Pair<String, BPlusTreeMetadata> resolveIndexMetadataFromName(String tableName,
                String columnName) {
            if (aliases.containsKey(tableName)) {
                tableName = aliases.get(tableName);
            }
            if (columnName.contains(".")) {
                String columnPrefix = columnName.split("\\.")[0];
                if (!tableName.equals(columnPrefix)) {
                    throw new DatabaseException("Column: " + columnName + " is not a column of " + tableName);
                }
                columnName = columnName.split("\\.")[1];
            }
            String indexName = tableName + "," + columnName;
            final String tableName1 = tableName;
            final String columnName1 = columnName;
            RecordId recordId = getIndexInfoRecordId(tableName, columnName);
            BPlusTreeMetadata metadata = getIndexMetadata(Database.this.indexInfo.getRecord(recordId));
            if (metadata == null) {
                throw new DatabaseException("no index with name " + indexName);
            }
            return new Pair<>(indexName, metadata);
        }

        private Pair<String, BPlusTree> resolveIndexFromName(String tableName,
                String columnName) {
            String indexName = resolveIndexMetadataFromName(tableName, columnName).getFirst();
            return new Pair<>(indexName, Database.this.indexLookup.get(indexName));
        }

        private Table getTable(String tableName) {
            if (this.aliases.containsKey(tableName)) {
                tableName = this.aliases.get(tableName);
            }

            if (this.tempTables.containsKey(tableName)) {
                return this.tempTables.get(tableName);
            }

            if (!tableName.startsWith(METADATA_TABLE_PREFIX)) {
                tableName = prefixUserTableName(tableName);
            }

            RecordId recordId = getTableInfoRecordId(tableName);
            TableInfoRecord record = new TableInfoRecord(Database.this.tableInfo.getRecord(recordId));
            if (!record.isAllocated()) {
                throw new DatabaseException("no table with name " + tableName);
            }
            return Database.this.tableLookup.get(tableName);
        }

        private String prefixTempTableName(String name) {
            String prefix = "temp." + transNum + "-";
            if (name.startsWith(prefix)) {
                return name;
            } else {
                return prefix + name;
            }
        }
    }

    private class TransactionImpl extends AbstractTransaction {
        private long transNum;
        private TransactionContext transactionContext;

        private TransactionImpl(long transNum) {
            this.transNum = transNum;
            this.transactionContext = new TransactionContextImpl(transNum);
        }

        @Override
        protected void startCommit() {
            this.prepareCommit();
            executor.execute(this::runCommit);
        }

        /**
         * Called before commit() returns to user.
         */
        private void prepareCommit() {
            // TODO(hw5): move cleanup() to runCommit and call into recoveryManager

            this.cleanup();
        }

        /**
         * Called after commit() returns to user.
         */
        private void runCommit() {
            // TODO(hw5): see prepareCommit
        }

        @Override
        protected void startRollback() {
            executor.execute(() -> {
                // TODO(hw5): call into recoveryManager before cleaning up

                this.cleanup();
            });

            throw new UnsupportedOperationException("TODO(hw5): implement");
        }

        private void cleanup() {
            transactionContext.close();

            // TODO(hw5): call into recoveryManager

            this.end();
            activeTransactions.arriveAndDeregister();
        }

        @Override
        public long getTransNum() {
            return transNum;
        }

        @Override
        public void createTable(Schema s, String tableName) {
            if (tableName.contains(".") && !tableName.startsWith("tables.")) {
                throw new IllegalArgumentException("name of new table may not contain '.'");
            }
            String prefixedTableName = prefixUserTableName(tableName);
            TransactionContext.setTransaction(transactionContext);
            try {
                int[] partNum = new int[1];
                long[] pageNum = new long[1];
                Database.this.tableInfoLookup.compute(prefixedTableName, (String t, RecordId recordId) -> {
                    if (recordId != null && new TableInfoRecord(tableInfo.getRecord(recordId)).isAllocated()) {
                        throw new DatabaseException("table " + t + " already exists");
                    }
                    partNum[0] = diskSpaceManager.allocPart();
                    pageNum[0] = diskSpaceManager.allocPage(partNum[0]);
                    List<DataBox> record = Arrays.asList(
                        new StringDataBox(prefixedTableName, 32),
                        new IntDataBox(partNum[0]),
                        new LongDataBox(pageNum[0]),
                        new BoolDataBox(false),
                        new IntDataBox(0),
                        new StringDataBox(new String(s.toBytes()), MAX_SCHEMA_SIZE)
                    );
                    if (recordId == null) {
                        return tableInfo.addRecord(record);
                    } else {
                        tableInfo.updateRecord(record, recordId);
                        return recordId;
                    }
                });
                LockContext tableContext = getTableContext(prefixedTableName, partNum[0]);
                HeapFile heapFile = new PageDirectory(bufferManager, partNum[0], pageNum[0], (short) 0,
                                                      tableContext);
                Database.this.tableLookup.put(prefixedTableName, new Table(prefixedTableName, s, heapFile,
                                              tableContext));
                Database.this.tableIndices.put(prefixedTableName, new ArrayList<>());
            } finally {
                TransactionContext.unsetTransaction();
            }
        }

        @Override
        public void dropTable(String tableName) {
            if (tableName.contains(".") && !tableName.startsWith("tables.")) {
                throw new IllegalArgumentException("name of table may not contain '.': " + tableName);
            }
            String prefixedTableName = prefixUserTableName(tableName);
            TransactionContext.setTransaction(transactionContext);
            try {
                // TODO(hw4_part2): add locking
                RecordId tableRecordId = Database.this.tableInfoLookup.compute(prefixedTableName,
                (String name, RecordId recordId) -> {
                    if (recordId == null) {
                        throw new DatabaseException("table " + name + " does not exist");
                    }
                    return recordId;
                });

                TableInfoRecord infoRecord = new TableInfoRecord(Database.this.tableInfo.getRecord(tableRecordId));
                if (!infoRecord.isAllocated()) {
                    throw new DatabaseException("table " + tableName + " does not exist");
                }
                Database.this.tableInfo.updateRecord(new TableInfoRecord(tableName).toDataBox(), tableRecordId);

                for (String indexName : new ArrayList<>(Database.this.tableIndices.get(prefixedTableName))) {
                    String[] parts = indexName.split(",");
                    dropIndex(parts[0], parts[1]);
                }

                Database.this.tableIndices.remove(prefixedTableName);
                Database.this.tableLookup.remove(prefixedTableName);
                bufferManager.freePart(infoRecord.partNum);
            } finally {
                TransactionContext.unsetTransaction();
            }
        }

        @Override
        public void dropAllTables() {
            TransactionContext.setTransaction(transactionContext);
            try {
                // TODO(hw4_part2): add locking

                List<String> tableNames = new ArrayList<>(tableLookup.keySet());

                for (String s : tableNames) {
                    if (s.startsWith("tables.")) {
                        this.dropTable(s);
                    }
                }
            } finally {
                TransactionContext.unsetTransaction();
            }
        }

        @Override
        public void createIndex(String tableName, String columnName, boolean bulkLoad) {
            if (tableName.contains(".") && !tableName.startsWith("tables.")) {
                throw new IllegalArgumentException("name of table may not contain '.'");
            }
            String prefixedTableName = prefixUserTableName(tableName);
            TransactionContext.setTransaction(transactionContext);
            try {
                // TODO(hw4_part2): add locking

                Schema s = getTable(tableName).getSchema();
                List<String> schemaColNames = s.getFieldNames();
                List<Type> schemaColType = s.getFieldTypes();

                if (!schemaColNames.contains(columnName)) {
                    throw new DatabaseException("table " + tableName + " does not have a column " + columnName);
                }

                int columnIndex = schemaColNames.indexOf(columnName);
                Type colType = schemaColType.get(columnIndex);
                String indexName = tableName + "," + columnName;

                int order = BPlusTree.maxOrder(BufferManager.EFFECTIVE_PAGE_SIZE, colType);
                BPlusTreeMetadata[] metadata = new BPlusTreeMetadata[1];
                indexInfoLookup.compute(indexName, (String name, RecordId recordId) -> {
                    if (recordId != null && getIndexMetadata(indexInfo.getRecord(recordId)) != null) {
                        throw new DatabaseException("index already exists on " + tableName + "(" + columnName + ")");
                    }
                    int partNum = diskSpaceManager.allocPart();
                    metadata[0] = new BPlusTreeMetadata(tableName, columnName, colType, order,
                                                        partNum, DiskSpaceManager.INVALID_PAGE_NUM, -1);
                    List<DataBox> record = Arrays.asList(
                        new StringDataBox(tableName, 32),
                        new StringDataBox(columnName, 32),
                        new IntDataBox(order),
                        new IntDataBox(partNum),
                        new LongDataBox(DiskSpaceManager.INVALID_PAGE_NUM),
                        new IntDataBox(colType.getTypeId().ordinal()),
                        new IntDataBox(colType.getSizeInBytes()),
                        new IntDataBox(-1)
                    );
                    if (recordId == null) {
                        return indexInfo.addRecord(record);
                    } else {
                        indexInfo.updateRecord(record, recordId);
                        return recordId;
                    }
                });
                LockContext indexContext = getIndexContext(indexName, metadata[0].getPartNum());
                TableInfoRecord info = new TableInfoRecord(tableInfo.getRecord(tableInfoLookup.get(
                            prefixedTableName)));
                ++info.numIndices;
                tableInfo.updateRecord(info.toDataBox(), tableInfoLookup.get(prefixedTableName));
                indexLookup.put(indexName, new BPlusTree(bufferManager, metadata[0], indexContext));
                tableIndices.get(prefixedTableName).add(indexName);

                // load data into index
                Table table = tableLookup.get(prefixedTableName);
                BPlusTree tree = indexLookup.get(indexName);
                if (bulkLoad) {
                    throw new UnsupportedOperationException("not implemented");
                } else {
                    for (RecordId rid : (Iterable<RecordId>) table::ridIterator) {
                        Record record = table.getRecord(rid);
                        tree.put(record.getValues().get(columnIndex), rid);
                    }
                }
            } finally {
                TransactionContext.unsetTransaction();
            }
        }

        @Override
        public void dropIndex(String tableName, String columnName) {
            String prefixedTableName = prefixUserTableName(tableName);
            TransactionContext.setTransaction(transactionContext);
            try {
                // TODO(hw4_part2): add locking

                String indexName = tableName +  "," + columnName;
                RecordId[] recordId = new RecordId[1];
                int partNum = indexLookup.compute(indexName, (String name, BPlusTree tree) -> {
                    if (tree == null) {
                        throw new DatabaseException("no index on " + tableName + "(" + columnName + ")");
                    }
                    recordId[0] = indexInfoLookup.get(name);
                    return tree;
                }).getPartNum();

                BPlusTree t = indexLookup.compute(indexName, (String name, BPlusTree tree) -> {
                    if (tree == null) {
                        throw new DatabaseException("no index on " + tableName + "(" + columnName + ")");
                    }

                    if (tree.getPartNum() != partNum) {
                        return tree;
                    }

                    tableIndices.get(prefixedTableName).remove(indexName);
                    return null;
                });

                if (t != null) {
                    // original index has been dropped, but a new index was made on the same column
                    dropIndex(tableName, columnName);
                    return;
                }

                indexInfoLookup.remove(indexName);
                BPlusTreeMetadata metadata = getIndexMetadata(indexInfo.deleteRecord(recordId[0]));
                TableInfoRecord info = new TableInfoRecord(tableInfo.getRecord(tableInfoLookup.get(
                            prefixedTableName)));
                --info.numIndices;
                tableInfo.updateRecord(info.toDataBox(), tableInfoLookup.get(prefixedTableName));

                bufferManager.freePart(metadata.getPartNum());
            } finally {
                TransactionContext.unsetTransaction();
            }
        }

        @Override
        public QueryPlan query(String tableName) {
            return new QueryPlan(transactionContext, tableName);
        }

        @Override
        public QueryPlan query(String tableName, String alias) {
            return new QueryPlan(transactionContext, tableName, alias);
        }

        @Override
        public void insert(String tableName, List<DataBox> values) {
            TransactionContext.setTransaction(transactionContext);
            try {
                transactionContext.addRecord(tableName, values);
            } finally {
                TransactionContext.unsetTransaction();
            }
        }

        @Override
        public void update(String tableName, String targetColumnName, UnaryOperator<DataBox> targetValue) {
            update(tableName, targetColumnName, targetValue, null, null, null);
        }

        @Override
        public void update(String tableName, String targetColumnName, UnaryOperator<DataBox> targetValue,
                           String predColumnName, PredicateOperator predOperator, DataBox predValue) {
            TransactionContext.setTransaction(transactionContext);
            try {
                transactionContext.runUpdateRecordWhere(tableName, targetColumnName, targetValue, predColumnName,
                                                        predOperator, predValue);
            } finally {
                TransactionContext.unsetTransaction();
            }
        }

        @Override
        public void delete(String tableName, String predColumnName, PredicateOperator predOperator,
                           DataBox predValue) {
            TransactionContext.setTransaction(transactionContext);
            try {
                transactionContext.runDeleteRecordWhere(tableName, predColumnName, predOperator, predValue);
            } finally {
                TransactionContext.unsetTransaction();
            }
        }

        @Override
        public void savepoint(String savepointName) {
            TransactionContext.setTransaction(transactionContext);
            try {
                recoveryManager.savepoint(transNum, savepointName);
            } finally {
                TransactionContext.unsetTransaction();
            }
        }

        @Override
        public void rollbackToSavepoint(String savepointName) {
            TransactionContext.setTransaction(transactionContext);
            try {
                recoveryManager.rollbackToSavepoint(transNum, savepointName);
            } finally {
                TransactionContext.unsetTransaction();
            }
        }

        @Override
        public void releaseSavepoint(String savepointName) {
            TransactionContext.setTransaction(transactionContext);
            try {
                recoveryManager.releaseSavepoint(transNum, savepointName);
            } finally {
                TransactionContext.unsetTransaction();
            }
        }

        @Override
        public Schema getSchema(String tableName) {
            return transactionContext.getSchema(tableName);
        }

        @Override
        public Schema getFullyQualifiedSchema(String tableName) {
            return transactionContext.getSchema(tableName);
        }

        @Override
        public TableStats getStats(String tableName) {
            return transactionContext.getStats(tableName);
        }

        @Override
        public int getNumDataPages(String tableName) {
            return transactionContext.getNumDataPages(tableName);
        }

        @Override
        public int getNumEntriesPerPage(String tableName) {
            return transactionContext.getNumEntriesPerPage(tableName);
        }

        @Override
        public int getEntrySize(String tableName) {
            return transactionContext.getEntrySize(tableName);
        }

        @Override
        public long getNumRecords(String tableName) {
            return transactionContext.getNumRecords(tableName);
        }

        @Override
        public int getTreeOrder(String tableName, String columnName) {
            return transactionContext.getTreeOrder(tableName, columnName);
        }

        @Override
        public int getTreeHeight(String tableName, String columnName) {
            return transactionContext.getTreeHeight(tableName, columnName);
        }

        @Override
        public TransactionContext getTransactionContext() {
            return transactionContext;
        }

        @Override
        public String toString() {
            return "Transaction " + transNum + " (" + getStatus().toString() + ")";
        }
    }
}
