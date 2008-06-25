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

import java.util.Arrays;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.join.AbstractAccessPath;
import com.bigdata.join.IAccessPath;
import com.bigdata.join.IChunkedIterator;
import com.bigdata.join.IKeyOrder;
import com.bigdata.join.IMutableRelation;
import com.bigdata.join.IPredicate;
import com.bigdata.join.ISolution;
import com.bigdata.join.SolutionComparator;

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
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SPORelation implements IMutableRelation<ISPO> {

    // @todo IRawTripleStore when re-factored back to the rdf module.
    private transient final long NULL = 0L;
    
    private final AbstractTripleStore tripleStore;
    
    public SPORelation(AbstractTripleStore tripleStore) {
        
        if (tripleStore == null)
            throw new IllegalArgumentException();
        
        this.tripleStore = tripleStore;
        
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
    
    public IAccessPath<ISPO> getAccessPath(SPOKeyOrder keyOrder,
            IPredicate<ISPO> pred) {

        final IIndex ndx = getIndex(keyOrder);

        final int flags = IRangeQuery.KEYS | IRangeQuery.VALS;
        
        return new SPOAccessPath(pred, keyOrder, ndx, flags);
        
    }
    
    /**
     * Return the {@link SPOKeyOrder} that will be used to read from the statement
     * index that is most efficient for the specified triple pattern.
     * 
     * @param s
     * @param p
     * @param o
     * @return
     */
    public IAccessPath<ISPO> getAccessPath(IPredicate<ISPO> pred) {

        final long s = pred.get(0).isVar() ? NULL : (Long) pred.get(0).get();
        final long p = pred.get(1).isVar() ? NULL : (Long) pred.get(1).get();
        final long o = pred.get(2).isVar() ? NULL : (Long) pred.get(2).get();

        if (s != NULL && p != NULL && o != NULL) {

            return getAccessPath(SPOKeyOrder.SPO, pred);

        } else if (s != NULL && p != NULL) {

            return getAccessPath(SPOKeyOrder.SPO, pred);

        } else if (s != NULL && o != NULL) {

            return getAccessPath(SPOKeyOrder.OSP, pred);

        } else if (p != NULL && o != NULL) {

            return getAccessPath(SPOKeyOrder.POS, pred);

        } else if (s != NULL) {

            return getAccessPath(SPOKeyOrder.SPO, pred);

        } else if (p != NULL) {

            return getAccessPath(SPOKeyOrder.POS, pred);

        } else if (o != NULL) {

            return getAccessPath(SPOKeyOrder.OSP, pred);

        } else {

            return getAccessPath(SPOKeyOrder.SPO, pred);

        }

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
     * @todo modify the mutation methods to write on all relevant indices in
     *       parallel. This is just a demonstration to prove out the API.
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
    public long insert(IChunkedIterator<ISolution<ISPO>> itr) {

        final ISolution<ISPO>[] chunk = itr.nextChunk();
        
        Arrays.sort(chunk, 0, chunk.length, new SolutionComparator<ISPO>(
                SPOKeyOrder.SPO));
        
        final IIndex ndx = getIndex(SPOKeyOrder.SPO);
        
        final ITupleSerializer<ISPO,ISPO> tupleSer = ndx.getIndexMetadata().getTupleSerializer();
        
        long n = 0;
        
        for(ISolution<ISPO> s : chunk) {
            
            final ISPO spo = s.get();

            final byte[] key = tupleSer.serializeKey(spo);

            if(!ndx.contains(key)) {

                final byte[] val = tupleSer.serializeVal(spo);
                
                ndx.insert(key, val);
                
                n++;
                
            }
            
        }
        
        return n;
        
    }

    public long remove(IChunkedIterator<ISolution<ISPO>> itr) {
        
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
        
    }

    public long update(IChunkedIterator<ISolution<ISPO>> itr,
            ITransform<ISPO> transform) {
        
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
        
    }

    private static class SPOAccessPath extends AbstractAccessPath<ISPO> {

        /**
         * @param predicate
         * @param keyOrder
         * @param ndx
         * @param flags
         */
        public SPOAccessPath(IPredicate<ISPO> predicate,
                IKeyOrder<ISPO> keyOrder, IIndex ndx, int flags) {

            super(predicate, keyOrder, ndx, flags);

        }

    }

}
