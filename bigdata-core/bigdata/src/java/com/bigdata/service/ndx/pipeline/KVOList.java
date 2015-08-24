package com.bigdata.service.ndx.pipeline;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

import com.bigdata.btree.keys.KVO;

/**
 * Extends {@link KVO} to allow duplicates to be gathered together in a
 * doubly-linked list. This is used to facilitate duplicate removal where the
 * goal is to eliminate the index write for the duplicate instance(s) but where
 * we still want to take some after action for each distinct instance.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @param <O>
 *            The generic type of the unserialized value object.
 * 
 * @see IDuplicateRemover
 */
public class KVOList<O> extends KVO<O> {

    /**
     * A doubly-linked chain.
     * <p>
     * Note: it is less expensive to allocate this with the {@link KVOList} than
     * to allocate an {@link AtomicReference} and then allocate this list lazily
     * but with guaranteed consistency.
     */
    private final ConcurrentLinkedQueue<KVO<O>> duplicateList = new ConcurrentLinkedQueue<KVO<O>>();

    private volatile boolean done = false;
    
    /**
     * 
     * @param key
     *            The unsigned byte[] key (required).
     * @param val
     *            The byte[] value (optional).
     * @param obj
     *            The paired application object (optional).
     * @param latch
     *            The object that maintains the counter.
     */
    public KVOList(final byte[] key, final byte[] val, final O obj) {

        super(key, val, obj);

    }

    /**
     * Add a reference to the duplicates list. This is MUTEX with
     * {@link #done()}.
     * 
     * @param o
     *            A duplicate of this object.
     */
    // Note: MUTUX with done().
    synchronized//
    public void add(final KVO<O> o) {

        if (o == null)
            throw new IllegalArgumentException();

        if(done)
            throw new IllegalStateException();
        
        duplicateList.add(o);

        if (o instanceof KVOList<?>) {

            /*
             * During a redirect, the last chunk written by the sink may be
             * combined with another chunk drained from the master. In these
             * cases we can see KVOLists identified as duplicates for KVOLists
             * which already have a list of duplicates. We handle this by
             * merging those lists.
             * 
             * Note: This is not a atomic operation if concurrent inserts are
             * allowed on the other KVOList. However, locking would make us open
             * to deadlock. The atomic guarantee arises from the fact that this
             * code is invoked from the sink, and the sink is single threaded,
             * so we don't really need all this synchronization in KVOList
             * anyway!
             * 
             * FIXME Review that MUXTEX guarantee!!!!
             * 
             * The atomic guarantee breaks down into three cases.
             * 
             * (1) The other list (the own passed in by the caller) is immutable
             * since this occurs when handling a stale locator exception, and
             * that behavior is single threaded. Hence the source list will not
             * have concurrent add()s.
             * 
             * (3) add() and done() must be mutually exclusive to ensure that
             * the behavior applied by done() is applied to all duplicates (this
             * is a race condition). The typical behavior is to decrement a
             * Latch.
             * 
             * (2) add() must not be allowed for this list once done() has been
             * invoked. The guarantee here arises from how duplicates are
             * detected. Duplicate detection occurs when taking a chunk from the
             * sink's queue. All items which can be duplicates wind up at the
             * same sink. The logic which takes a chunk from the sink's queue
             * and eliminates any duplicates is single threaded.
             * 
             * The second problem is making sure that no new
             * 
             * @todo Review this guarantee if we move to an NIO model for the
             * data transfers for the sinks. It will probably still be Ok since
             * the single threaded concerns here are the transfer of chunks from
             * the master and the handling of stale locator exceptions. In both
             * of those cases the other list (the one being added to this list)
             * will be immutable.
             */

            final KVOList<O> t = (KVOList<O>) o;

            duplicateList.addAll(t.duplicateList);

            t.duplicateList.clear();

        }

    }

    /**
     * The #of duplicates on the internal list. This will report ZERO (0) if
     * there are no duplicates (the internal list is empty).
     */
    public int getDuplicateCount() {
        
        return duplicateList.size();
        
    }

    /**
     * Return <code>true</code> iff no duplicates have been assigned.
     */
    public boolean isDuplicateListEmpty() {

        return duplicateList.isEmpty();
        
    }
    
    /**
     * Extended to map the operation over the duplicate list.
     */
    @Override
    // Note: MUTEX with add(o)
    synchronized//
    public void done() {

        if(done)
            throw new IllegalStateException();
        
        done = true;
        
        super.done();

        for (KVO<O> o : duplicateList) {

            o.done();

        }
   
    }

    /**
     * An operation which can be mapped over the duplicate list.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     * @param <O>
     */
    static public interface Op<O> {
        
        public void apply(KVO<O> o);
        
    }

    /**
     * Maps the operation across the duplicate list (the operation is NOT
     * applied to the original).
     * 
     * @param op
     *            The operation.
     */
    public void map(final Op<O> op) {
        
        if (op == null)
            throw new IllegalArgumentException();
        
//            op.apply(this);

        for (KVO<O> o : duplicateList) {

            op.apply( o );

        }

    }
    
}
