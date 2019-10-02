package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;

public class DummyRecoveryManager implements RecoveryManager {
    @Override
    public void initialize() {}

    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {}

    @Override
    public void startTransaction(Transaction transaction) {}

    @Override
    public long commit(long transNum) {
        return 0;
    }

    @Override
    public long abort(long transNum) {
        throw new UnsupportedOperationException("hw5 must be implemented to use abort");
    }

    @Override
    public long end(long transNum) {
        return 0;
    }

    @Override
    public void pageFlushHook(long pageNum, long pageLSN) {}

    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        return 0;
    }

    @Override
    public long logAllocPart(long transNum, int partNum) {
        return 0;
    }

    @Override
    public long logFreePart(long transNum, int partNum) {
        return 0;
    }

    @Override
    public long logAllocPage(long transNum, long pageNum) {
        return 0;
    }

    @Override
    public long logFreePage(long transNum, long pageNum) {
        return 0;
    }

    @Override
    public void logDiskIO(long pageNum) {}

    @Override
    public void savepoint(long transNum, String name) {
        throw new UnsupportedOperationException("hw5 must be implemented to use savepoints");
    }

    @Override
    public void releaseSavepoint(long transNum, String name) {
        throw new UnsupportedOperationException("hw5 must be implemented to use savepoints");
    }

    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        throw new UnsupportedOperationException("hw5 must be implemented to use savepoints");
    }

    @Override
    public void checkpoint() {
        throw new UnsupportedOperationException("hw5 must be implemented to use checkpoints");
    }

    @Override
    public Runnable restart() {
        return () -> {};
    }

    @Override
    public void close() {}
}
