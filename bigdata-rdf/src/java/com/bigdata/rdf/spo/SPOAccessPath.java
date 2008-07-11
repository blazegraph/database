package com.bigdata.rdf.spo;

import com.bigdata.btree.IIndex;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.relation.accesspath.AbstractAccessPath;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IChunkedOrderedIterator;
import com.bigdata.relation.accesspath.IKeyOrder;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IVariableOrConstant;

/**
 * {@link IAccessPath} implementation for an {@link SPORelation}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SPOAccessPath extends AbstractAccessPath<SPO> {

    private static transient final long NULL = IRawTripleStore.NULL;

    private SPOTupleSerializer tupleSer;
    
    /**
     * Set by the ctor to the term identifier appearing in the subject position
     * or {@link IRawTripleStore#NULL} if the subject position is unbound.
     */
    public final long s;

    /**
     * Set by the ctor to the term identifier appearing in the predicate
     * position or {@link IRawTripleStore#NULL} if the predicate position is
     * unbound.
     */
    public final long p;

    /**
     * Set by the ctor to the term identifier appearing in the object position
     * or {@link IRawTripleStore#NULL} if the object position is unbound.
     */
    public final long o;

    /**
     * Note: Filters should be specified when the {@link IAccessPath} is
     * constructed so that they will be evalated on the data service rather than
     * materializing the elements and then filtering then. This can be
     * accomplished by adding the filter as a constraint on the predicate when
     * specifying the access path.
     * 
     * @param predicate
     * @param keyOrder
     * @param ndx
     * @param flags
     */
    public SPOAccessPath(SPORelation relation, IPredicate<SPO> predicate,
            IKeyOrder<SPO> keyOrder, IIndex ndx, int flags) {

        super(relation, predicate, keyOrder, ndx, flags);

        {

            final IVariableOrConstant<Long> t = predicate.get(0);

            s = t.isVar() ? NULL : t.get();

        }

        {

            final IVariableOrConstant<Long> t = predicate.get(1);

            p = t.isVar() ? NULL : t.get();

        }
        
        {

            final IVariableOrConstant<Long> t = predicate.get(2);

            o = t.isVar() ? NULL : t.get();

        }

    }

//    /**
//     * Helper method returns {@link IRawTripleStore#NULL} if the argument is an
//     * {@link IVariable} and otherwise the value of the {@link IConstant}.
//     * 
//     * @param t
//     *            Some variable or constant.
//     */
//    static public long asLong(IVariableOrConstant<Long> t) {
//
//        return t.isVar() ? NULL : t.get();
//
//    }
    
    protected SPOTupleSerializer getTupleSerializer() {

        if (tupleSer == null) {

            tupleSer = (SPOTupleSerializer) ndx.getIndexMetadata()
                    .getTupleSerializer();

        }

        return tupleSer;
        
    }
    
    public SPOAccessPath init() {

        final SPOTupleSerializer tupleSer = getTupleSerializer();
        
        final byte[] fromKey;
        final byte[] toKey;
        
        if (s != NULL && p != NULL && o != NULL) {
            
            assert keyOrder == SPOKeyOrder.SPO;
            
            fromKey = tupleSer.statement2Key(s, p, o);

            toKey = tupleSer.statement2Key(s, p, o + 1);

        } else if (s != NULL && p != NULL) {

            assert keyOrder == SPOKeyOrder.SPO;
            
            fromKey = tupleSer.statement2Key(s, p, NULL);

            toKey = tupleSer.statement2Key(s, p + 1, NULL);

        } else if (s != NULL && o != NULL) {

            assert keyOrder == SPOKeyOrder.OSP;
            
            fromKey = tupleSer.statement2Key(o, s, NULL);

            toKey = tupleSer.statement2Key(o, s + 1, NULL);

        } else if (p != NULL && o != NULL) {

            assert keyOrder == SPOKeyOrder.POS;
            
            fromKey = tupleSer.statement2Key(p, o, NULL);

            toKey = tupleSer.statement2Key(p, o + 1, NULL);

        } else if (s != NULL) {

            assert keyOrder == SPOKeyOrder.SPO;
            
            fromKey = tupleSer.statement2Key(s, NULL, NULL);

            toKey = tupleSer.statement2Key(s + 1, NULL, NULL);

        } else if (p != NULL) {

            assert keyOrder == SPOKeyOrder.POS;
            
            fromKey = tupleSer.statement2Key(p, NULL, NULL);

            toKey = tupleSer.statement2Key(p + 1, NULL, NULL);

        } else if (o != NULL) {

            assert keyOrder == SPOKeyOrder.OSP;
            
            fromKey = tupleSer.statement2Key(o, NULL, NULL);

            toKey = tupleSer.statement2Key(o + 1, NULL, NULL);

        } else {

            /*
             * Note: The KeyOrder does not matter when you are fully
             * unbound.
             */
            
            fromKey = toKey = null;

        }

        setFromKey(fromKey);

        setToKey(toKey);

        super.init();
    
        return this;
        
    }

    /**
     * Strenghened return type.
     */
    public SPORelation getRelation() {
        
        return (SPORelation) super.getRelation();
        
    }
    
    /**
     * Overriden to delegate to
     * {@link AbstractTripleStore#removeStatements(IChunkedOrderedIterator)} in
     * order to (a) write on all access paths; (b) handle statement identifiers,
     * including truth maintenance for statement identifiers; and (c) if
     * justifications are being maintained, then retract justifications having
     * no support once the statements visitable by this access path have been
     * retracted.
     */
    @Override
    public long removeAll() {
        
        final SPORelation r = getRelation();

        final AbstractTripleStore db = r.getContainer();

        return db.removeStatements(iterator());
        
    }
    
}
