package com.bigdata.join;

/**
 * A factory for fused views reading from both of the source {@link IRelation}s.
 * 
 * FIXME re-factor into an {@link IRelationName} view and the fused view of the
 * resolved relations.
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
            throw new IllegalArgumentException();
        
        this.relation1 = relation1;
        
        this.relation2 = relation2;
        
    }
    
    /**
     * @todo it may be necessary to override predicate#getrelation() in
     *       order to have the predicate directed to each of the source
     *       relations specify that relation in its getRelation() method.
     */
    public IAccessPath<E> getAccessPath(IPredicate<E> predicate) {

        return new AccessPathFusedView<E>(relation1.getAccessPath(predicate), relation2
                .getAccessPath(predicate));
        
    }

    /**
     * Note: You can not compute the exact element count for a fused view since
     * there may be duplicate elements in the two source {@link IRelation}s.
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
    
}
