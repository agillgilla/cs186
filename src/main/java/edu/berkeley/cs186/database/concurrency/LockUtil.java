package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.ArrayDeque;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock acquisition
 * for the user (you, in the second half of Part 2). Generally speaking, you should use LockUtil
 * for lock acquisition instead of calling LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring LOCKTYPE on LOCKCONTEXT.
     *
     * This method should promote/escalate as needed, but should only grant the least
     * permissive set of locks needed.
     *
     * lockType is guaranteed to be one of: S, X, NL.
     *
     * If the current transaction is null (i.e. there is no current transaction), this method should do nothing.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType lockType) {
        // TODO(hw4_part2): implement

        // This seems hacky, but had to add it to pass HW4 Part 2 Task 4
        if (lockContext.readonly) {
            return;
        }

        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction
        if (transaction == null) {
            return;
        }
        // If the requested lock type is NL, we don't need to do anything
        if (lockType == LockType.NL) {
            return;
        }
        // If the lock type is already granted (either explicitly or effectively) we don't need to do anything
        if (LockType.substitutable(lockContext.getExplicitLockType(transaction), lockType) ||
                LockType.substitutable(lockContext.getEffectiveLockType(transaction), lockType)) {
            return;
        }

        // Do auto-escalation if necessary
        checkAndPerformAutoEscalate(lockContext, transaction);

        // If auto-escalation granted us the lock type we need (either explicitly or effectively) we don't need to do anything
        if (LockType.substitutable(lockContext.getExplicitLockType(transaction), lockType) ||
                LockType.substitutable(lockContext.getEffectiveLockType(transaction), lockType)) {
            return;
        }

        ensureAncestorSufficientLockHeld(transaction, lockContext.parentContext(), LockType.parentLock(lockType));

        if (lockContext.getExplicitLockType(transaction) == LockType.NL) {
            //System.out.println("FINAL ACQUISITION OF " + lockContext.getResourceName() + " OF LEVEL: " + lockType);
            lockContext.acquire(transaction, lockType);
        } else {
            if (LockType.substitutable(lockType, lockContext.getEffectiveLockType(transaction))) {
                //System.out.println("Lock of type " + lockType + " is substitutable for held type: " + lockContext.getEffectiveLockType(transaction));
                lockContext.promote(transaction, lockType);
            } else {
                if ((lockContext.getExplicitLockType(transaction) == LockType.S && lockType == LockType.IX) ||
                        (lockType == LockType.S && lockContext.getExplicitLockType(transaction) == LockType.IX)) {
                    //System.out.println("PROMOTING SPECIFIC SIX CASE");
                    lockContext.promote(transaction, LockType.SIX);
                } else {
                    //System.out.println("ESCALATING THE SITUATION...");
                    //System.out.flush();
                    lockContext.escalate(transaction);
                }
            }
        }

        //System.out.println("=============================================");
        //System.out.flush();

    }

    public static void ensureSufficientLockStateForChildren(LockContext parentLockContext, LockType childLockType) {
        LockType parentLockType = LockType.parentLock(childLockType);

        // This seems hacky, but had to add it to pass HW4 Part 2 Task 4
        if (parentLockContext.readonly) {
            return;
        }

        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction
        if (transaction == null) {
            return;
        }
        // If the requested lock type is NL, we don't need to do anything
        if (childLockType == LockType.NL) {
            return;
        }
        // If the lock type is already granted (either explicitly or effectively) we don't need to do anything
        if (LockType.substitutable(parentLockContext.getExplicitLockType(transaction), parentLockType) ||
                LockType.substitutable(parentLockContext.getEffectiveLockType(transaction), parentLockType)) {
            return;
        }

        // Do auto-escalation if necessary
        checkAndPerformAutoEscalate(parentLockContext, transaction);

        // If auto-escalation granted us the lock type we need (either explicitly or effectively) we don't need to do anything
        if (LockType.substitutable(parentLockContext.getExplicitLockType(transaction), parentLockType) ||
                LockType.substitutable(parentLockContext.getEffectiveLockType(transaction), parentLockType)) {
            return;
        }

        ensureAncestorSufficientLockHeld(transaction, parentLockContext, parentLockType);
    }

    // TODO(hw4_part2): add helper methods as you see fit

    private static void ensureAncestorSufficientLockHeld(TransactionContext transaction, LockContext lockContext, LockType lockType) {
        ArrayDeque<Pair<LockContext, LockType>> ancestorChain = new ArrayDeque<>();

        LockType currParentLockType = lockType;
        LockContext currParentLockContext = lockContext;

        while (currParentLockContext != null) {
            //System.out.println("ADDING TO ACQUIRE LIST: " + currParentLockContext.getResourceName());
            //System.out.flush();
            ancestorChain.addFirst(new Pair(currParentLockContext, currParentLockType));

            currParentLockContext = currParentLockContext.parentContext();
            currParentLockType = LockType.parentLock(lockType);
        }

        for (Pair<LockContext, LockType> ancestorAndLockType : ancestorChain) {
            LockContext ancestorLockContext = ancestorAndLockType.getFirst();
            LockType ancestorLockType = ancestorAndLockType.getSecond();

            // If we already have an effective lock type that is good enough, don't do anything
            if (LockType.substitutable(ancestorLockContext.getEffectiveLockType(transaction), ancestorLockType)) {
                continue;
            }

            if (ancestorLockContext.getEffectiveLockType(transaction).equals(LockType.NL)) {
                //System.out.println("ACQUIRING: " + ancestorLockContext.getResourceName() + " WITH LEVEL: " + ancestorLockType);
                //System.out.flush();
                ancestorLockContext.acquire(transaction, ancestorLockType);
            } else {
                try {
                    if ((ancestorLockContext.getExplicitLockType(transaction) == LockType.S && ancestorLockType == LockType.IX) ||
                            (ancestorLockType == LockType.S && ancestorLockContext.getExplicitLockType(transaction) == LockType.IX)) {
                        //System.out.println("PROMOTING ANCESTOR SPECIFIC SIX CASE");
                        ancestorLockContext.promote(transaction, LockType.SIX);
                    } else {
                        //System.out.println("PROMOTING: " + ancestorLockContext.getResourceName() + " TO LEVEL: " + ancestorLockType);
                        //System.out.flush();
                        ancestorLockContext.promote(transaction, ancestorLockType);
                    }
                }  catch (DuplicateLockRequestException dre) {
                    continue;
                } catch (NoLockHeldException nhe) {
                    //System.out.println("Escalating " + ancestorLockContext.getResourceName());
                    ancestorLockContext.escalate(transaction);
                } catch (InvalidLockException ile) {
                    //System.out.println("Escalating " + ancestorLockContext.getResourceName());
                    ancestorLockContext.escalate(transaction);
                }
            }
        }
    }

    private static void checkAndPerformAutoEscalate(LockContext lockContext, TransactionContext transaction) {
        LockContext parentContext = lockContext.parentContext();

        // Only auto escalate if the parent has auto escalate enabled (which implies the parent LockContext is for a table)
        if (parentContext != null && parentContext.isAutoEscalateEnabled()) {
            //System.out.println("GOT TO FIRST STATE OF AUTO ESCALATE...");
            // Check the criteria to perform auto escalate
            if (parentContext.saturation(transaction) >= 0.2 && parentContext.capacity() >= 10) {
                //System.out.println("AUTO ESCALATING....");
                //System.out.flush();
                lockContext.parentContext().escalate(transaction);
            }
        }
        //System.out.flush();
    }
}
