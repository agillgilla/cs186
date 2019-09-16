package edu.berkeley.cs186.database;

public abstract class AbstractTransaction implements Transaction {
    private Status status = Status.RUNNING;

    /**
     * Called when commit() is called. Any exception thrown in this method will cause
     * the transaction to abort.
     */
    protected abstract void startCommit();

    /**
     * Called when rollback() is called. No exception should be thrown, and any exception
     * thrown will be interpreted the same as if the method had returned normally.
     */
    protected abstract void startRollback();

    /**
     * Commit the transaction.
     */
    @Override
    public final void commit() {
        if (status != Status.RUNNING) {
            throw new IllegalStateException("transaction not in running state, cannot commit");
        }
        startCommit();
        status = Status.COMMITTING;
    }

    /**
     * Rollback the transaction.
     */
    @Override
    public final void rollback() {
        if (status != Status.RUNNING) {
            throw new IllegalStateException("transaction not in running state, cannot rollback");
        }
        startRollback();
        status = Status.ABORTING;
    }

    protected final void end() {
        status = Status.COMPLETE;
    }

    @Override
    public final Status getStatus() {
        return status;
    }

    /**
     * Implements close() as commit() when abort/commit not called - so that we can write:
     * <p>
     * try (Transaction t = ...) {
     * ...
     * }
     * <p>
     * and have the transaction commit.
     */
    @Override
    public final void close() {
        if (status == Status.RUNNING) {
            commit();
        }
    }
}
