package com.bigdata.relation;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.relation.accesspath.AccessPathFusedView;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IPredicate;

/**
 * A factory for fused views reading from both of the source {@link IRelation}s.
 *  
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 */
public class RelationFusedView<E> implements IRelation<E> {
    
    private IRelation<E> relation1;
    private IRelation<E> relation2;
    
    public IRelation<E> getRelation1() {
        
        return relation1;
        
    }
    
    public IRelation<E> getRelation2() {
        
        return relation2;
        
    }
    
    /**
     * 
     * @param relation1
     * @param relation2
     */
    public RelationFusedView(IRelation<E> relation1, IRelation<E> relation2) {
        
        if (relation1 == null)
            throw new IllegalArgumentException();
        
        if (relation2 == null)
            throw new IllegalArgumentException();

        if (relation1 == relation2)
            throw new IllegalArgumentException("same relation: " + relation1);
        
        this.relation1 = relation1;
        
        this.relation2 = relation2;
        
    }
    
    public IAccessPath<E> getAccessPath(IPredicate<E> predicate) {

        return new AccessPathFusedView<E>(//
                relation1.getAccessPath(predicate),//
                relation2.getAccessPath(predicate)//
        );
        
    }

    /**
     * Note: You can not compute the exact element count for a fused view since
     * there may be duplicate elements in the two source {@link IRelation}s
     * (well, you could merge the data in the views into a temporary view but
     * that is hardly efficient).
     * 
     * @throws UnsupportedOperationException
     *             if <code>exact == true</code>.
     */
    public long getElementCount(boolean exact) {

        if (exact) {

            throw new UnsupportedOperationException();
            
        }
        
        return relation1.getElementCount(exact)
                + relation2.getElementCount(exact);
        
    }

    public Set<String> getIndexNames() {
        
        final Set<String> set = new HashSet<String>();
        
        set.addAll(relation1.getIndexNames());

        set.addAll(relation2.getIndexNames());
        
        return set;
    
    }

    public ExecutorService getExecutorService() {
        
        return relation1.getExecutorService();
        
    }
    
    public Object newElement(IPredicate predicate, IBindingSet bindingSet) {

        return relation1.newElement(predicate, bindingSet);

    }

    /**
     * The {@link IIndexManager} for the first relation in the view.
     */
    public IIndexManager getIndexManager() {

        return relation1.getIndexManager();

    }

    /*
     * Note: These methods can not be implemented for the fused view.
     */

    /**
     * Not implemented for a fused view. (The elements of the view can have
     * different timestamps - this is especially true when one is a
     * {@link TemporaryStore}.)
     * 
     * @throws UnsupportedOperationException
     *             always.
     */
    public long getTimestamp() {
        
        throw new UnsupportedOperationException();
        
    }

    /**
     * Not implemented for a fused view.
     * 
     * @throws UnsupportedOperationException
     *             always.
     */
    public String getNamespace() {

        throw new UnsupportedOperationException();

    }

    /**
     * Not implemented for a fused view.
     * 
     * @throws UnsupportedOperationException
     *             always.
     */
    public String getContainerNamespace() {
    
        throw new UnsupportedOperationException();
        
    }

}
