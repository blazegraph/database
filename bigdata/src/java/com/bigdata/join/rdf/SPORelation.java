/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

*/
/*
 * Created on Jun 21, 2008
 */

package com.bigdata.join.rdf;

import org.apache.log4j.Logger;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.join.Constant;
import com.bigdata.join.IAccessPath;
import com.bigdata.join.IChunkedOrderedIterator;
import com.bigdata.join.IMutableRelation;
import com.bigdata.join.IPredicate;
import com.bigdata.join.ISolution;
import com.bigdata.join.IVariableOrConstant;
import com.bigdata.join.Var;

/**
 * A relation corresponding to the triples in a triple store.
 * <p>
 * A triple store is modeled as a relation of arity THREE (3) with N access
 * paths, one for each statement index (the arity is actually either 4 or 5
 * since the statement metadata includes whether it is {explicit, inferred, or
 * axiom} and the statement identifier is optionally replicated into the
 * relation as well). The term2id and id2term indices are a 2nd relation of
 * arity TWO (2) between the term and the term identifer. The full text index is
 * a third relation. When querying a focusStore and a db, the focusStore will
 * have the same relation class (the triples) and the index classes declared
 * (SPO, POS, OSP) but the relation instance and the indices will be distinct
 * from those associated with the main db. The justifications index is also a
 * secondary index for the relation. Its contents are the binding sets for the
 * solutions computed for the rules during truth maintenance.
 * 
 * @todo Re-factor and integrate the AbstractTripleStore.
 * 
 * @todo I have pulled out the [justify] flag as it is not general purpose. A
 *       justification is comprised exactly from the tail bindings since they
 *       are what justifies the head. Writing the justifications onto an index
 *       is an optional action that is performed with the selected bindings, so
 *       it really has to do with index maintenance for the {@link SPORelation}.
 *       <p>
 *       The {explicit,inferred,axiom} marker needs to be set to [inferred] when
 *       the rule generated the bindings for the triple.
 * 
 * @todo integration with package providing magic set rewrites of rules in order
 *       to test whether or not a statement is still provable when it is
 *       retracted during TM.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SPORelation implements IMutableRelation<SPO> {

    protected static final Logger log = Logger.getLogger(SPORelation.class);
    
    // @todo IRawTripleStore when re-factored back to the rdf module.
    private transient final long NULL = 0L;
    
    private final TestTripleStore tripleStore;
    
    private final SPORelationName relationName;
    
    public SPORelation(TestTripleStore tripleStore) {
        
        if (tripleStore == null)
            throw new IllegalArgumentException();
        
        this.tripleStore = tripleStore;
        
        this.relationName = new SPORelationName(tripleStore.getNamespace());
        
    }
    
    /**
     * @todo integrate with triple store impls to returns the appropriate
     *       statement index. Add ctor accepting the IRawTripleStore and use
     *       getStatementIndex(String name) to obtain the appropriate index.
     *       This works if we assume that the triple store is fully indexed.
     */
    public IIndex getIndex(SPOKeyOrder keyOrder) {

        return tripleStore.getStatementIndex(keyOrder);
        
    }
    
    /**
     * 
     * @param s
     * @param p
     * @param o
     */
    public IAccessPath<SPO> getAccessPath(final long s, final long p, final long o) {

        final IVariableOrConstant<Long> S = (s == NULL ? Var.var("s")
                : new Constant<Long>(s));

        final IVariableOrConstant<Long> P = (p == NULL ? Var.var("p")
                : new Constant<Long>(p));

        final IVariableOrConstant<Long> O = (o == NULL ? Var.var("o")
                : new Constant<Long>(o));
        
        return getAccessPath(new SPOPredicate(relationName, S, P, O));
        
    }

    /**
     * Return the {@link IAccessPath} that is most efficient for the specified
     * predicate based on an analysis of the bound and unbound positions in the
     * predicate.
     * 
     * @return The best access path for that predicate.
     */
    public IAccessPath<SPO> getAccessPath(final IPredicate<SPO> predicate) {

        if (predicate == null)
            throw new IllegalArgumentException();
        
        final long s = predicate.get(0).isVar() ? NULL : (Long) predicate.get(0).get();
        final long p = predicate.get(1).isVar() ? NULL : (Long) predicate.get(1).get();
        final long o = predicate.get(2).isVar() ? NULL : (Long) predicate.get(2).get();

        final IAccessPath<SPO> accessPath;
        
        if (s != NULL && p != NULL && o != NULL) {

            accessPath = getAccessPath(SPOKeyOrder.SPO, predicate);

        } else if (s != NULL && p != NULL) {

            accessPath = getAccessPath(SPOKeyOrder.SPO, predicate);

        } else if (s != NULL && o != NULL) {

            accessPath = getAccessPath(SPOKeyOrder.OSP, predicate);

        } else if (p != NULL && o != NULL) {

            accessPath = getAccessPath(SPOKeyOrder.POS, predicate);

        } else if (s != NULL) {

            accessPath = getAccessPath(SPOKeyOrder.SPO, predicate);

        } else if (p != NULL) {

            accessPath = getAccessPath(SPOKeyOrder.POS, predicate);

        } else if (o != NULL) {

            accessPath = getAccessPath(SPOKeyOrder.OSP, predicate);

        } else {

            accessPath = getAccessPath(SPOKeyOrder.SPO, predicate);

        }
        
        if (log.isDebugEnabled()) {

            log.debug(accessPath.toString());
            
        }
        
        return accessPath;
        
    }

    /**
     * Core impl.
     * 
     * @param keyOrder
     *            The natural order of the selected index (this identifies the
     *            index).
     * @param predicate
     *            The predicate specifying the query constraint on the access
     *            path.
     * @return The access path.
     */
    public IAccessPath<SPO> getAccessPath(SPOKeyOrder keyOrder,
            IPredicate<SPO> predicate) {

        if (keyOrder == null)
            throw new IllegalArgumentException();
        
        if (predicate == null)
            throw new IllegalArgumentException();
        
        final IIndex ndx = getIndex(keyOrder);

        final int flags = IRangeQuery.KEYS | IRangeQuery.VALS;
        
        return new SPOAccessPath(tripleStore.getThreadPool(), predicate,
                keyOrder, ndx, flags).init();
        
    }
    
    public long getElementCount(boolean exact) {

        final IIndex ndx = getIndex(SPOKeyOrder.SPO);
        
        if (exact) {
        
            return ndx.rangeCountExact(null/* fromKey */, null/* toKey */);
            
        } else {
            
            return ndx.rangeCount(null/* fromKey */, null/* toKey */);
            
        }
        
    }

    /**
     * FIXME modify the mutation methods to write on all relevant indices in
     * parallel. This is just a demonstration to prove out the API.
     * 
     * @todo if justifications are being maintained then the {@link ISolution}s
     *       MUST report binding sets and we will also write on the
     *       justifications index.
     * 
     * @todo The existing triple store addStatements(ISPOIterator) can be
     *       modified to insert(IChunkedIterator<ISPO>) and common code for
     *       parallel index writes can be shared between the two methods. The
     *       only difference is that the addStatements(...) method does not have
     *       justification metadata and therefore must not be used to write
     *       inferences onto the database (today it works differently and the
     *       caller runs two tasks in parallel where one runs the procedure that
     *       writes on the statement indices in parallel while the other runs
     *       the procedure to write on the justifications index).
     */
    public long insert(IChunkedOrderedIterator<SPO> itr) {

        final IIndex[] ndx = new IIndex[] {
                getIndex(SPOKeyOrder.SPO),
                getIndex(SPOKeyOrder.POS),
                getIndex(SPOKeyOrder.OSP)
        };

        try {
            
            final SPO[] chunk = itr.nextChunk(SPOKeyOrder.SPO);

            long n = 0;

            for (SPO spo : chunk) {

                // FIXME not testing for pre-existence.
//                if (!ndx[0].contains(key)) {
                    
                    for(IIndex x : ndx) {

                        final SPOTupleSerializer tupleSer = (SPOTupleSerializer) x
                                .getIndexMetadata().getTupleSerializer();

                        final byte[] key = tupleSer.serializeKey(spo);

                        final byte[] val = tupleSer.serializeVal(spo);

                        x.insert(key, val);

                    }

                    n++;

//                }

            }

            return n;

        } finally {

            itr.close();
            
        }
        
    }
    
    public long remove(IChunkedOrderedIterator<SPO> itr) {
        
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
        
    }

    public long update(IChunkedOrderedIterator<SPO> itr,
            ITransform<SPO> transform) {
        
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
        
    }

}
