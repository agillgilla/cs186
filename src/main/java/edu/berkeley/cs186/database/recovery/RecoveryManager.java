package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;

/**
 * Interface for a recovery manager.
 */
public interface RecoveryManager extends AutoCloseable {
    /**
     * Initializes the log; only called the first time the database is set up.
     */
    void initialize();

    /**
     * Sets the buffer/disk managers. This is not part of the constructor because of the cyclic dependency
     * between the buffer manager and recovery manager (the buffer manager must interface with the
     * recovery manager to block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager);

    /**
     * Adds transaction to transaction table.
     * @param transaction new transaction to add to transaction table
     */
    void startTransaction(Transaction transaction);

    /**
     * Write a commit record to the log, and flush it to disk before returning.
     * @param transNum transaction being committed
     */
    long commit(long transNum);

    /**
     * Writes an abort record to the log.
     * @param transNum transaction being aborted
     */
    long abort(long transNum);

    /**
     * Cleans up and ends the transaction. This method should write any needed
     * CLRs to the log, as well as the END record.
     * @param transNum transaction to finish
     */
    long end(long transNum);

    /**
     * Called before a page is flushed from the buffer cache. The log must be flushed
     * up to the pageLSN of the page before the page is flushed.
     * @param pageNum page number of page about to be flushed
     * @param pageLSN pageLSN of page about to be flushed
     */
    void pageFlushHook(long pageNum, long pageLSN);

    /**
     * Log a write to a page. Should do nothing and return -1 if the page is a log page. If the record
     * would be too big (length * 2 > effective page size), then an undo-only update record is written,
     * followed by a redo-only update record.
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of newest record
    */
    long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                      byte[] after);

    /**
     * Logs a partition allocation, and flushes the log.
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record
     */
    long logAllocPart(long transNum, int partNum);

    /**
     * Logs a partition free, and flushes the log.
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record
     */
    long logFreePart(long transNum, int partNum);

    /**
     * Logs a page allocation, and flushes the log.
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record
     */
    long logAllocPage(long transNum, long pageNum);

    /**
     * Logs a page free, and flushes the log.
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record
     */
    long logFreePage(long transNum, long pageNum);

    /**
     * Called on a successful flush.
     * @param pageNum page number of page that was flushed
     */
    void logDiskIO(long pageNum);

    /**
     * Creates a savepoint for a transaction.
     * @param transNum transaction to make savepoint for
     * @param name name of savepoint
     */
    void savepoint(long transNum, String name);

    /**
     * Releases (deletes) a savepoint for a transaction.
     * @param transNum transaction to delete savepoint for
     * @param name name of savepoint
     */
    void releaseSavepoint(long transNum, String name);

    /**
     * Rolls back transaction to a savepoint
     * @param transNum transaction to partially rollback
     * @param name name of savepoint
     */
    void rollbackToSavepoint(long transNum, String name);

    /**
     * Starts the checkpointing process.
     */
    void checkpoint();

    /**
     * This method is called whenever the database starts up, aside from the very first run
     * (when there is no log at all), and performs recovery.
     * @return task to run to finish restart recovery
     */
    Runnable restart();

    @Override
    void close();
}
