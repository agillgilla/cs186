package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import org.omg.CORBA.DynAnyPackage.Invalid;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have
 * what locks on what resources. The lock manager should generally **not**
 * be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with
 * multiple levels of granularity (you can and should treat ResourceName
 * as a generic Object, rather than as an object encapsulating levels of
 * granularity, in this class).
 *
 * It follows that LockManager should allow **all**
 * requests that are valid from the perspective of treating every resource
 * as independent objects, even if they would be invalid from a
 * multigranularity locking perspective. For example, if LockManager#acquire
 * is called asking for an X lock on Table A, and the transaction has no
 * locks at the time, the request is considered valid (because the only problem
 * with such a request would be that the transaction does not have the appropriate
 * intent locks, but that is a multigranularity concern).
 *
 * Each resource the lock manager manages has its own queue of LockRequest objects
 * representing a request to acquire (or promote/acquire-and-release) a lock that
 * could not be satisfied at the time. This queue should be processed every time
 * a lock on that resource gets released, starting from the first request, and going
 * in order until a request cannot be satisfied. Requests taken off the queue should
 * be treated as if that transaction had made the request right after the resource was
 * released in absence of a queue (i.e. removing a request by T1 to acquire X(db) should
 * be treated as if T1 had just requested X(db) and there were no queue on db: T1 should
 * be given the X lock on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();
    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // TODO(hw4_part1): You may add helper methods here if you wish

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                   ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<Long, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to NAME.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    // TODO(hw4_part1): You may add helper methods here if you wish

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION, and releases all locks
     * in RELEASELOCKS after acquiring the lock, in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * Locks in RELEASELOCKS should be released only after the requested lock has been acquired.
     * The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on NAME **should not** change the
     * acquisition time of the lock on NAME, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), acquire X(A) and release S(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by TRANSACTION and
     * isn't being released
     * @throws NoLockHeldException if no lock on a name in RELEASELOCKS is held by TRANSACTION
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseLocks)
    throws DuplicateLockRequestException, NoLockHeldException {
        // TODO(hw4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.

        Lock lockToAcquire = new Lock(name, lockType, transaction.getTransNum());
        boolean isCompatible = true;

        synchronized (this) {

            // Acquire the lock
            for (Map.Entry<Long, List<Lock>> entry : transactionLocks.entrySet()) {
                Long transactionNum = entry.getKey();
                List<Lock> locksHeld = entry.getValue();

                if (transactionNum == transaction.getTransNum()) {
                    for (Lock lockHeld : locksHeld) {
                        if (name.equals(lockHeld.name) && !releaseLocks.contains(name)) {
                            throw new DuplicateLockRequestException("Attempt to request duplicate lock for resource: " + name);
                        }
                    }
                } else {
                    for (Lock lockHeld : locksHeld) {
                        if (name.equals(lockHeld.name)) {
                            if (!LockType.compatible(lockHeld.lockType, lockType)) {
                                isCompatible = false;

                                transaction.prepareBlock();
                                this.getResourceEntry(name).waitingQueue.addFirst(new LockRequest(transaction, lockToAcquire));
                            }
                        }
                    }
                }
            }

            if (isCompatible) {
                List<Lock> locksHeld = this.getLocks(transaction);

                if (locksHeld.size() == 0) {
                    transactionLocks.put(transaction.getTransNum(), new ArrayList<Lock>(Arrays.asList(lockToAcquire)));
                } else {
                    locksHeld.add(lockToAcquire);
                }
                this.getResourceEntry(name).locks.add(lockToAcquire);
            }


            // Release the lock(s)
            for (int i = 0; i < releaseLocks.size(); i++) {
                ResourceName lockToRelease = releaseLocks.get(i);

                if (!transactionLocks.containsKey(transaction.getTransNum())) {
                    throw new NoLockHeldException("Attempt to release lock not held for resource: " + lockToRelease);
                }

                boolean lockWasHeld = false;

                List<Lock> locksHeld = this.getLocks(transaction);

                for (Lock lockHeld : locksHeld) {
                    if (lockHeld.name.equals(lockToRelease)) {
                        if (isCompatible) {
                            // Release the lock that the transaction has
                            transactionLocks.get(transaction.getTransNum()).remove(lockHeld);
                            this.getResourceEntry(lockToRelease).locks.remove(lockHeld);

                            // Process the queue for the resource
                            processResourceQueue(lockToRelease);
                        }
                        lockWasHeld = true;
                    }
                }

                if (!lockWasHeld) {
                    throw new NoLockHeldException("Attempt to release lock not held for resource: " + lockToRelease);
                }
            }
        }

        if (!isCompatible) {
            transaction.block();
        }
    }

    private void processResourceQueue(ResourceName name) {
        // We use an iterator instead of a for loop to prevent ConcurrentModificationException
        Iterator<LockRequest> lockRequestIterator = this.getResourceEntry(name).waitingQueue.iterator();
        while (lockRequestIterator.hasNext()) {
            LockRequest lockRequest = lockRequestIterator.next();

            List<Lock> locksToRelease = lockRequest.releasedLocks;

            List<Lock> locksGranted = this.getResourceEntry(lockRequest.lock.name).locks;

            boolean satisfiable = true;
            boolean promotion = false;

            for (Lock grantedLock : locksGranted) {
                if (!LockType.compatible(grantedLock.lockType, lockRequest.lock.lockType)) {
                    // If we're coing to release the lock, it doesn't matter if it is compatible
                    if (!locksToRelease.contains(grantedLock)) {
                        satisfiable = false;
                    } else {
                        promotion = true;
                    }
                }
            }

            if (!satisfiable) {
                // Stop processing by breaking from loop
                break;
            } else {
                TransactionContext transactionToRelease = lockRequest.transaction;

                if (promotion) {
                    transactionLocks.get(transactionToRelease.getTransNum()).remove(locksToRelease.get(0));
                    this.getResourceEntry(locksToRelease.get(0).name).locks.remove(locksToRelease.get(0));
                }

                lockRequest.transaction.unblock();

                acquire(lockRequest.transaction, lockRequest.lock.name, lockRequest.lock.lockType);

                lockRequestIterator.remove();
            }
        }
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by
     * TRANSACTION
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // TODO(hw4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.

        Lock lockToAcquire = new Lock(name, lockType, transaction.getTransNum());
        boolean isCompatibleAndNoQueue = true;

        synchronized (this) {
            // Acquire the lock
            for (Map.Entry<Long, List<Lock>> entry : transactionLocks.entrySet()) {
                Long transactionNum = entry.getKey();
                List<Lock> locksHeld = entry.getValue();

                if (transactionNum == transaction.getTransNum()) {
                    for (Lock lockHeld : locksHeld) {
                        if (name.equals(lockHeld.name)) {
                            throw new DuplicateLockRequestException("Attempt to request duplicate lock for resource: " + name + ". Held: " + lockHeld.lockType + ", Requested: " + lockType);
                        }
                    }
                } else {
                    for (Lock lockHeld : locksHeld) {
                        if (name.equals(lockHeld.name)) {
                            if (!LockType.compatible(lockHeld.lockType, lockType) || this.getResourceEntry(name).waitingQueue.size() > 0) {
                                isCompatibleAndNoQueue = false;

                                transaction.prepareBlock();
                                this.getResourceEntry(name).waitingQueue.addLast(new LockRequest(transaction, lockToAcquire));
                            }
                        }
                    }
                }
            }

            if (isCompatibleAndNoQueue) {
                List<Lock> locksHeld = this.getLocks(transaction);

                if (locksHeld.size() == 0) {
                    transactionLocks.put(transaction.getTransNum(), new ArrayList<Lock>(Arrays.asList(lockToAcquire)));
                } else {
                    locksHeld.add(lockToAcquire);
                }
                this.getResourceEntry(name).locks.add(lockToAcquire);
            }
        }

        if (!isCompatibleAndNoQueue) {
            transaction.block();
        }
    }

    /**
     * Release TRANSACTION's lock on NAME.
     *
     * Error checking must be done before the lock is released.
     *
     * NAME's queue should be processed after this call. If any requests in
     * the queue have locks to be released, those should be released, and the
     * corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     */
    public void release(TransactionContext transaction, ResourceName name)
    throws NoLockHeldException {
        // TODO(hw4_part1): implement
        // You may modify any part of this method.
        synchronized (this) {

            // Release the lock
            ResourceName lockToRelease = name;

            if (!transactionLocks.containsKey(transaction.getTransNum())) {
                throw new NoLockHeldException("Attempt to release lock not held for resource: " + lockToRelease);
            }

            boolean lockWasHeld = false;

            List<Lock> locksHeld = this.getLocks(transaction);

            for (Lock lockHeld : locksHeld) {
                if (lockHeld.name.equals(lockToRelease)) {
                    // Release the lock that the transaction has
                    transactionLocks.get(transaction.getTransNum()).remove(lockHeld);
                    this.getResourceEntry(lockToRelease).locks.remove(lockHeld);

                    // Process the queue for the resource
                    processResourceQueue(lockToRelease);

                    lockWasHeld = true;
                }
            }

            if (!lockWasHeld) {
                throw new NoLockHeldException("Attempt to release lock not held for resource: " + lockToRelease);
            }
        }
    }

    /**
     * Promote TRANSACTION's lock on NAME to NEWLOCKTYPE (i.e. change TRANSACTION's lock
     * on NAME from the current lock type to NEWLOCKTYPE, which must be strictly more
     * permissive).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * A lock promotion **should not** change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a
     * NEWLOCKTYPE lock on NAME
     * @throws NoLockHeldException if TRANSACTION has no lock on NAME
     * @throws InvalidLockException if the requested lock type is not a promotion. A promotion
     * from lock type A to lock type B is valid if and only if B is substitutable
     * for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(hw4_part1): implement
        // You may modify any part of this method.

        boolean isCompatible = true;
        Lock lockToPromote = new Lock(name, newLockType, transaction.getTransNum());
        Lock lockToRelease = null;

        synchronized (this) {
            // Check that lock is currently held
            if (!transactionLocks.containsKey(transaction.getTransNum())) {
                throw new NoLockHeldException("Attempt to promote lock not held for resource: " + name);
            }

            boolean lockWasHeld = false;
            List<Lock> locksAlreadyHeld = this.getLocks(transaction);

            for (Lock lockHeld : locksAlreadyHeld) {
                if (lockHeld.name.equals(name)) {
                    lockWasHeld = true;

                    lockToRelease = lockHeld;

                    // Check that lock promotion is valid
                    if (!LockType.substitutable(newLockType, lockHeld.lockType)) {
                        throw new InvalidLockException("Attempt to promote a lock to non-substitutable type: " + newLockType + " from " + lockHeld.lockType);
                    }
                }
            }

            if (!lockWasHeld) {
                throw new NoLockHeldException("Attempt to promote lock not held for resource: " + name);
            }


            // Check lock compatibility
            for (Map.Entry<Long, List<Lock>> entry : transactionLocks.entrySet()) {
                Long transactionNum = entry.getKey();
                List<Lock> locksHeld = entry.getValue();

                if (transactionNum == transaction.getTransNum()) {
                    for (Lock lockHeld : locksHeld) {
                        if (name.equals(lockHeld.name) & newLockType == lockHeld.lockType) {
                            throw new DuplicateLockRequestException("Attempt to promote duplicate lock for resource: " + name);
                        }
                    }
                } else {
                    for (Lock lockHeld : locksHeld) {
                        if (name.equals(lockHeld.name)) {
                            if (!LockType.compatible(lockHeld.lockType, newLockType)) {
                                isCompatible = false;

                                transaction.prepareBlock();
                                this.getResourceEntry(name).waitingQueue.addFirst(new LockRequest(transaction, lockToPromote, new ArrayList<>(Arrays.asList(lockToRelease))));
                            }
                        }
                    }
                }
            }

            if (isCompatible) {
                List<Lock> locksHeld = this.getLocks(transaction);

                // Release the lock that the transaction has
                int locksHeldIndex = locksHeld.indexOf(lockToRelease);
                locksHeld.remove(locksHeldIndex);
                int resourceIndex = this.getResourceEntry(lockToRelease.name).locks.indexOf(lockToRelease);
                this.getResourceEntry(lockToRelease.name).locks.remove(resourceIndex);

                // Acquire the promoted lock
                locksHeld.add(locksHeldIndex, lockToPromote);
                this.getResourceEntry(name).locks.add(resourceIndex, lockToPromote);
            }

        }

        if (!isCompatible) {
            transaction.block();
        }
    }

    /**
     * Return the type of lock TRANSACTION has on NAME (return NL if no lock is held).
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        // TODO(hw4_part1): implement

        ResourceEntry resourceEntry = resourceEntries.get(name);

        if (resourceEntry != null) {
            for (Lock lockHeld : resourceEntry.locks) {
                if (lockHeld.transactionNum == transaction.getTransNum()) {
                    return lockHeld.lockType;
                }
            }
        }

        return LockType.NL;
    }

    /**
     * Returns the list of locks held on NAME, in order of acquisition.
     * A promotion or acquire-and-release should count as acquired
     * at the original time.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks locks held by
     * TRANSACTION, in order of acquisition. A promotion or
     * acquire-and-release should count as acquired at the original time.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                               Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at
     * he top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext context(String readable, long name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, new Pair<>(readable, name)));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at
     * the top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database", 0L);
    }
}
