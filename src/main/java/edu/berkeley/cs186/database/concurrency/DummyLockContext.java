package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

/**
 * A lock context that doesn't do anything at all. Used where a lock context
 * is expected, but no locking should be done.
 */
public class DummyLockContext extends LockContext {
    public DummyLockContext() {
        this(null);
    }

    public DummyLockContext(LockContext parent) {
        super(new DummyLockManager(), parent, null);
    }

    @Override
    public void acquire(TransactionContext transaction, LockType lockType)
    throws InvalidLockException, DuplicateLockRequestException { }

    @Override
    public void release(TransactionContext transaction)
    throws NoLockHeldException, InvalidLockException { }

    @Override
    public void promote(TransactionContext transaction, LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException { }

    @Override
    public void escalate(TransactionContext transaction) throws NoLockHeldException { }

    @Override
    public void disableChildLocks() { }

    @Override
    public LockContext childContext(String readable, long name) { return new DummyLockContext(this); }

    @Override
    public int capacity() {
        return 0;
    }

    @Override
    public void capacity(int capacity) {
    }

    @Override
    public double saturation(TransactionContext transaction) {
        return 0.0;
    }

    @Override
    public LockType getExplicitLockType(TransactionContext transaction) {
        return LockType.NL;
    }

    @Override
    public LockType getEffectiveLockType(TransactionContext transaction) {
        return LockType.NL;
    }

    @Override
    public String toString() {
        return "Dummy Lock Context";
    }
}

