package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     * <p>
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException          if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     *                                       transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        if (readonly) {
                //&& (lockType.equals(LockType.X) || lockType.equals(LockType.IX))) {
            throw new UnsupportedOperationException("Lock context is read only");
        }
        if (hasSIXAncestor(transaction) && (lockType.equals(LockType.IS) || lockType.equals(LockType.S))) {
            throw new InvalidLockException("Request is redundant");
        }

        if (this.parentContext() != null) {
            LockType parentLockType = this.lockman.getLockType(transaction, this.parentContext().getResourceName());
            //LockType parentLockType = this.parent.getEffectiveLockType(transaction);

            if (!LockType.canBeParentLock(parentLockType, lockType)) {
                throw new InvalidLockException("Invalid lock type: current context holding " +
                        parentLockType.toString() + " requesting " + lockType.toString());
            }
        }
        lockman.acquire(transaction, name, lockType);
        if (this.parentContext() != null) {
            Long transNum = transaction.getTransNum();
            int numLocks = this.parentContext().numChildLocks.getOrDefault(transNum, 0);
            this.parentContext().numChildLocks.put(transNum, numLocks + 1);
        }
        //update number of child locks
//        LockContext currParent = parent;
//        while (currParent != null) {
//            Long transNum = transaction.getTransNum();
//            int numLocks = parent.numChildLocks.getOrDefault(transNum, 0);
//            parent.numChildLocks.put(transNum, numLocks + 1);
//            currParent = currParent.parent;
//        }
        return;
    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        if (readonly) {
            throw new UnsupportedOperationException("Lock context is read only");
        }
        if (getExplicitLockType(transaction).equals(LockType.NL)) {
            throw new NoLockHeldException("No lock held on " + name.toString());
        }
        if (!numChildLocks.getOrDefault(transaction.getTransNum(), 0).equals(0)) {
            throw new InvalidLockException("Child context is still holding locks");
        }
        lockman.release(transaction, this.name);
        decreaseNumChildLocks(transaction, parent);
        return;
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        if (readonly) {
            throw new UnsupportedOperationException("Lock context is read only");
        }
        if (this.parentContext() != null) {
            LockType parentLockType = this.lockman.getLockType(transaction, this.name);
            if (!LockType.canBeParentLock(parentLockType, newLockType)) {
                throw new InvalidLockException("Invalid request " + newLockType + "lock context holds " + parentLockType);
            }
        }
        if (newLockType == LockType.SIX) {
            List<ResourceName> resourceNames = this.sisDescendants(transaction);
            if (!resourceNames.isEmpty()) {

            }
        }
//        LockType type = getEffectiveLockType(transaction);
//        if (type.equals(LockType.NL)) {
//            throw new NoLockHeldException("No lock is held");
//        }
//        if (type.equals(newLockType)) {
//            throw new DuplicateLockRequestException("Lock already in place, no need to promote");
//        }
//        if (!newLockType.equals(LockType.SIX)) {
//            if (LockType.substitutable(newLockType, type)) {
//                lockman.promote(transaction, name, newLockType);
//            } else {
//                throw new InvalidLockException("Invalid promotion type");
//            }
//        } else {
//            List<ResourceName> toRelease = sisDescendants(transaction);
//            List<LockContext> contexts = new ArrayList<>();
//            toRelease.add(name);
//            for (ResourceName resName : toRelease) {
//                contexts.add(fromResourceName(lockman, resName));
//            }
//            lockman.acquireAndRelease(transaction, name, newLockType, toRelease);
//            for (LockContext context : contexts) {
//                LockContext currParent = context.parent;
//                while (currParent != null) {
//                    Long transNum = transaction.getTransNum();
//                    int numLocks = currParent.parent.numChildLocks.getOrDefault(transNum, 0);
//                    if (numLocks == 0) break;
//                    currParent.numChildLocks.put(transNum, numLocks - 1);
//                    currParent = currParent.parent;
//                }
//            }
            return;
        //}
    }

//    private void updateNumChildLocks(TransactionContext transactionContext) {
//        if (this.numChildLocks != null && !this.numChildLocks.isEmpty() && this.numChildLocks.containsKey(transactionContext.getTransNum())) {
//            int before
//        }
//    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        if (readonly) {
            throw new UnsupportedOperationException("Lock context is read only");
        }
        LockType type = getEffectiveLockType(transaction);
        if (type.equals(LockType.NL)) {
            throw new NoLockHeldException("No lock is held");
        }
        if (type.equals(LockType.S) || type.equals(LockType.X)) {
            return;
        }

        List<ResourceName> toRelease = listAllDescendants(transaction);
        toRelease.add(name);
        List<LockContext> contexts = new ArrayList<>();
        for (ResourceName resourceName : toRelease) {
            LockContext context = fromResourceName(lockman, resourceName);
            contexts.add(context);
        }

        if (type.equals(LockType.IS)) {
            lockman.acquireAndRelease(transaction, name, LockType.S, toRelease);
            for (LockContext context : contexts) {
                context.decreaseNumChildLocks(transaction, context.parent);
            }
        } else if (type.equals(LockType.IX) || type.equals(LockType.SIX)) {
            lockman.acquireAndRelease(transaction,name, LockType.X, toRelease);
            for (LockContext context : contexts) {
                context.decreaseNumChildLocks(transaction, context.parent);
            }
        }
        return;
    }

    private void decreaseNumChildLocks(TransactionContext transaction, LockContext context) {
        if (context == null) {
            return;
        }
        Long transNum = transaction.getTransNum();
        int numLocks = context.getNumChildren(transaction);
        if (numLocks != 0) {
            context.numChildLocks.put(transNum, numLocks - 1);
            decreaseNumChildLocks(transaction, context.parent);
        }
        return;
    }
    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;

        return lockman.getLockType(transaction, getResourceName());
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        LockType lockType = this.getExplicitLockType(transaction);
        LockContext pt = this.parent;
        while (pt != null) {
            if (!pt.getExplicitLockType(transaction).equals(LockType.NL)){
                break;
            }
            pt = pt.parent;
        }
        if (pt == null) return LockType.NL;
        LockType ancestorLockType = pt.getExplicitLockType(transaction);
        if (ancestorLockType.equals(LockType.SIX)) {
            return LockType.S;
        } else if (ancestorLockType.equals(LockType.IS) || ancestorLockType.equals(LockType.IX)) {
            return LockType.NL;
        }
        return ancestorLockType;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        LockContext context = parent;
        while (context != null) {
            LockType parentLockType = lockman.getLockType(transaction, context.getResourceName());
            if (parentLockType.equals(LockType.SIX)) {
                return true;
            }
            context = context.parent;
        }
        return false;
    }

    private List<ResourceName> listAllDescendants(TransactionContext transaction) {
        List<Lock> locks = lockman.getLocks(transaction);
        List<ResourceName> names = new ArrayList<>();
        for (Lock lock : locks) {
            LockContext context = fromResourceName(lockman, lock.name);
            if (isAncestor(context, this)) {
                names.add(lock.name);
            }
        }
        return names;
    }
    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        List<Lock> locks = lockman.getLocks(transaction);
        List<ResourceName> names = new ArrayList<>();
        for (Lock lock : locks) {
            if (lock.lockType.equals(LockType.S) || lock.lockType.equals(LockType.IS)) {
                LockContext context = fromResourceName(lockman, lock.name);
                if (isAncestor(context, this)) {
                    names.add(lock.name);
                }

            }
        }
        return names;
    }

    private boolean isAncestor(LockContext child, LockContext ancestor) {
        if (child == null) {
            return false;
        }
        if (ancestor == null) {
            return false;
        }
        LockContext pt = child.parent;
        while (pt != null) {
            if (pt.equals(ancestor)) {
                return true;
            }
            pt = pt.parent;
        }
        return false;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}
