package com.bigdata.service.ndx.pipeline;

import java.util.LinkedList;

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
     * A doubly-linked chain. Access to this object MUST be protected by
     * synchronization on the {@link KVOList}.
     */
    private LinkedList<KVO<O>> duplicateList;

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
    public void add(final KVO<O> o) {

        if (o == null)
            throw new IllegalArgumentException();

        if (o == this)
            throw new IllegalArgumentException();

        /*
         * Do not permit KVOList having assigned duplicates to be added as
         * duplicates to another KVOList.
         */
        if (o instanceof KVOList) {
            synchronized (o) {
                if (((KVOList) o).duplicateList != null)
                    throw new IllegalStateException();
            }
        }
        
        synchronized(this) {
            
            if(duplicateList == null) {
                
                duplicateList = new LinkedList<KVO<O>>();
                
            }

            duplicateList.add( o );
            
        }
        
    }

    /**
     * The #of duplicates on the internal list. This will report ZERO (0) if
     * there are no duplicates (the internal list is empty).
     */
    public int getDuplicateCount() {
        
        synchronized(this) {
            
            if(duplicateList == null)
                return 0;
            
            return duplicateList.size();
            
        }
        
    }

    /**
     * Return <code>true</code> iff no duplicates have been assigned.
     */
    public boolean isDuplicateListEmpty() {

        synchronized (this) {

            return duplicateList == null;
            
        }
        
    }
    
    /**
     * Extended to map the operation over the list.
     */
    @Override
    public void done() {

        synchronized (this) {

            super.done();

            if (duplicateList != null) {

                for (KVO<O> o : duplicateList) {

                    o.done();

                }

            }

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
        
        synchronized (this) {

//            op.apply(this);

            if (duplicateList != null) {

                for (KVO<O> o : duplicateList) {

                    op.apply( o );

                }

            }

        }

    }
    
}
