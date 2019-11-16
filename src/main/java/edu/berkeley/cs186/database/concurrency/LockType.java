package edu.berkeley.cs186.database.concurrency;

public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(hw4_part1): implement

        if (b == NL) {
            return true;
        }

        switch(a) {
            case S:
                return b == IS || b == S;
            case X:
                return false;
            case IS:
                return b != X;
            case IX:
                return b == IS || b == IX;
            case SIX:
                return b == IS;
            case NL:
                return true;
            default:
                return false;
        }
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(hw4_part1): implement

        if (childLockType == IS || childLockType == S) {
            return parentLockType == IS || parentLockType == IX;
        } else if (childLockType == IX || childLockType == SIX || childLockType == X) {
            return parentLockType == IX || parentLockType == SIX;
        } else {
            // Child is NL
            return true;
        }
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(hw4_part1): implement

        if (required == substitute) {
            // If substitute is same, then it is definitely substitutable
            return true;
        } else if (substitute == NL) {
            // No lock can ever substitute for anything (except no lock)
            return false;
        }

        if (required == S) {
            return substitute == X || substitute == SIX;
        } else  if (required == X) {
            return false;
        } else if (required == IS) {
            return substitute == IX || substitute == SIX;
        } else if (required == IX) {
            return substitute == SIX;
        } else if (required == SIX) {
            return false;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

