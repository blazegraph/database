package com.bigdata.join.rdf;

import java.util.concurrent.ExecutorService;

import com.bigdata.btree.IIndex;
import com.bigdata.join.AbstractAccessPath;
import com.bigdata.join.IAccessPath;
import com.bigdata.join.IKeyOrder;
import com.bigdata.join.IPredicate;
import com.bigdata.join.IVariableOrConstant;

/**
 * {@link IAccessPath} implementation for an {@link SPORelation}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SPOAccessPath extends AbstractAccessPath<SPO> {

    // @todo IRawTripleStore when re-factored back to the rdf module.
    private transient final long NULL = 0L;

    private SPOTupleSerializer tupleSer;
    
    /**
     * Set by the ctor.
     */
    protected final long s, p, o;
    
    /**
     * @param predicate
     * @param keyOrder
     * @param ndx
     * @param flags
     */
    public SPOAccessPath(ExecutorService service, IPredicate<SPO> predicate,
            IKeyOrder<SPO> keyOrder, IIndex ndx, int flags) {

        super(service, predicate, keyOrder, ndx, flags);

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
    
    protected SPOTupleSerializer getTupleSerializer() {

        if (tupleSer == null) {

            tupleSer = (SPOTupleSerializer) ndx.getIndexMetadata()
                    .getTupleSerializer();

        }

        return tupleSer;
        
    }
    
//    protected byte[] getKey(IPredicate<SPO> pred) {
//
//        return getKey(toElement(pred));
//
//    }
//
//    protected byte[] getKey(SPO spo) {
//
//        final byte[] key = getTupleSerializer().serializeKey(spo);
//
//        return key;
//        
//    }

//    /**
//     * The successor of the {@link SPO} in the {@link SPOKeyOrder} for this
//     * access path.
//     * 
//     * @param spo An {@link SPO}.
//     * 
//     * @return Its successor for this {@link IAccessPath}.
//     */
//    protected SPO successor(SPO spo) {
//       
//        if (spo == null)
//            throw new IllegalArgumentException();
//        
//        switch (((SPOKeyOrder) keyOrder).index()) {
//        
//        case SPOKeyOrder._SPO:
//
//            return new SPO(spo.s, spo.p, spo.o + 1);
//
//        case SPOKeyOrder._POS:
//
//            return new SPO(spo.s + 1, spo.p, spo.o);
//
//        case SPOKeyOrder._OSP:
//
//            return new SPO(spo.s, spo.p + 1, spo.o);
//            
//        default: throw new AssertionError();
//        
//        }
//        
//    }
    
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
    
}
