/**

 Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on March 19, 2008
 */
package com.bigdata.rdf.inf;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.TempTripleStore;
import com.bigdata.relation.accesspath.IChunkedOrderedIterator;
import com.bigdata.relation.accesspath.IKeyOrder;

/**
 * Provides backward chaining for reverse property collection on owl:sameAs for
 * the ?PO and ??O access paths.
 * <p>
 * Note: This is a reverse properties query: we know o and and we want to know
 * all {s,p} tuples that constitute the reverse properties for o.
 * 
 * @see InferenceEngine
 * @see InferenceEngine.Options
 * 
 * @author <a href="mailto:mpersonick@users.sourceforge.net">Mike Personick</a>
 */
public class BackchainOwlSameAsPropertiesPOIterator extends
        BackchainOwlSameAsIterator {
    
    private IChunkedOrderedIterator<SPO> sameAs3It, sameAs2It;

    private TempTripleStore sameAs3, sameAs2;

    private boolean canRemove = false;

    /**
     * Create an iterator that will visit all statements in the source iterator
     * and also backchain any entailments that would have resulted from
     * owl:sameAs {2,3}.
     * 
     * @param src
     *            The source iterator. {@link #nextChunk()} will sort statements
     *            into the {@link IKeyOrder} reported by this iterator (as long
     *            as the {@link IKeyOrder} is non-<code>null</code>).
     * @param p
     *            The predicate of the triple pattern. Can be null.
     * @param o
     *            The object of the triple pattern. Cannot be null.
     * @param db
     *            The database from which we will read the distinct subject
     *            identifiers (iff this is an all unbound triple pattern).
     * @param sameAs
     *            The term identifier that corresponds to owl:sameAs for the
     *            database.
     */
    public BackchainOwlSameAsPropertiesPOIterator(IChunkedOrderedIterator<SPO> src, long p,
            long o, AbstractTripleStore db, final long sameAs) {
        super(src, db, sameAs);
        Properties props = db.getProperties();
        // do not store terms
        props.setProperty(AbstractTripleStore.Options.LEXICON, "false");
        // only store the SPO index
        props.setProperty(AbstractTripleStore.Options.ONE_ACCESS_PATH, "true");
        sameAs3 = new TempTripleStore(props,db);
        sameAs2 = new TempTripleStore(props,db);
        /*
         * Collect up additional reverse properties (s and p values) for the
         * known o value by examining the values which are owl:sameAs o. The p
         * might or might not be bound in this access path.
         */
        {
            // join:
            // ( o sameAs ?same ) x ( ?s p ?same )
            // to produce ( ?s p o )
            // which might be present in the source iterator already
            // use a buffer so that we can do a more efficient batch contains
            // to filter out existing statements
            int chunkSize = 10000;
            SPO[] spos = new SPO[chunkSize];
            int numSPOs = 0;
            // get all of o's sames
            Iterator<Long> samesIt = getSames(o).iterator();
            while (samesIt.hasNext()) {
                long same = samesIt.next();
                // attach all of the same's reverse properties to o
                IChunkedOrderedIterator<SPO> reversePropsIt = db.getAccessPath(NULL, p, same).iterator();
                while (reversePropsIt.hasNext()) {
                    SPO reverseProp = reversePropsIt.next();
                    // do not add ( s sameAs s ) inferences
                    if (reverseProp.p == sameAs && reverseProp.s == o) {
                        continue;
                    }
                    // flush the buffer if necessary
                    if (numSPOs == chunkSize) {
                        boolean present = false; // filter for not present
                        IChunkedOrderedIterator<SPO> absent = db.bulkFilterStatements(spos,
                                numSPOs, present);
                        db.addStatements(sameAs3,copyOnly,absent, null);
                        numSPOs = 0;
                    }
                    // attach the s and p to the original o
                    spos[numSPOs++] = new SPO(reverseProp.s, reverseProp.p, o,
                            StatementEnum.Inferred);
                }
            }
            // final flush of the buffer
            boolean present = false; // filter for not present
            IChunkedOrderedIterator<SPO> absent = db.bulkFilterStatements(spos, numSPOs,
                    present);
            db.addStatements(sameAs3,copyOnly,absent, null);
        }
        sameAs3It = sameAs3.getAccessPath(SPOKeyOrder.SPO).iterator();
    }

    public IKeyOrder<SPO> getKeyOrder() {
        
        return src.getKeyOrder();
        
    }

    public boolean hasNext() {
        
        if (src.hasNext() || sameAs3It.hasNext()) {
            
            return true;
            
        } else if (sameAs2It == null) {
            
            sameAs2It = sameAs2.getAccessPath(SPOKeyOrder.SPO).iterator();
            
        }

        return sameAs2It.hasNext();
        
    }

    /**
     * First iterate the source iterator and then iterate the sameAs{3}
     * iterator, which was fully populated in the ctor. Along the way, collect
     * up the sameAs{2} inferences, which will then be iterated once the first
     * two iterators are complete.
     */
    public SPO next() {
        canRemove = false;
        SPO current = null;
        if (src.hasNext()) {
            current = src.next();
            processSameAs2(current);
            canRemove = true;
        } else if (sameAs3It.hasNext()) {
            current = sameAs3It.next();
            processSameAs2(current);
        } else {
            if (sameAs2It == null) {
                sameAs2It = sameAs2.getAccessPath(SPOKeyOrder.SPO).iterator();
            }
            if (sameAs2It.hasNext()) {
                current = sameAs2It.next();
            }
        }
        return current;
    }

    /**
     * Find all the alternate s values for this SPO, which we need to do since s
     * is unbound in this access path.
     * 
     * @param spo
     *            the spo being visited by the source iterator or the sameAs{3}
     *            iterator
     */
    private void processSameAs2(SPO spo) {
        // join:
        // ( s p o ) x ( s sameAs ?same )
        // to produce ( ?same p o )
        // which might be present in the source iterator already
        // ignore sameAs properties
        // use a buffer so that we can do a more efficient batch contains
        // to filter out existing statements
        int chunkSize = 10000;
        SPO[] spos = new SPO[chunkSize];
        int numSPOs = 0;
        // get all of s's sames
        Iterator<Long> samesIt = getSames(spo.s).iterator();
        while (samesIt.hasNext()) {
            long same = samesIt.next();
            // do not add ( s sameAs s ) inferences
            if (spo.p == sameAs && same == spo.o) {
                continue;
            }
            // flush the buffer if necessary
            if (numSPOs == chunkSize) {
                boolean present = false; // filter for not present
                IChunkedOrderedIterator<SPO> absent = db.bulkFilterStatements(spos, numSPOs,
                        present);
                db.addStatements(sameAs2,copyOnly,absent, null);
                numSPOs = 0;
            }
            // attach the new s to the original p and o
            spos[numSPOs++] = new SPO(same, spo.p, spo.o,
                    StatementEnum.Inferred);
        }
        // final flush of the buffer
        boolean present = false; // filter for not present
        IChunkedOrderedIterator<SPO> absent = db.bulkFilterStatements(spos, numSPOs, present);
        db.addStatements(sameAs2,copyOnly,absent, null);
    }

    public SPO[] nextChunk() {
        final int chunkSize = 10000;
        SPO[] s = new SPO[chunkSize];
        int n = 0;
        while (hasNext() && n < chunkSize) {
            s[n++] = next();
        }
        SPO[] stmts = new SPO[n];
        // copy so that stmts[] is dense.
        System.arraycopy(s, 0, stmts, 0, n);
        return stmts;
    }

    public SPO[] nextChunk(IKeyOrder<SPO> keyOrder) {
        if (keyOrder == null)
            throw new IllegalArgumentException();
        SPO[] stmts = nextChunk();
        if (src.getKeyOrder() != keyOrder) {
            // sort into the required order.
            Arrays.sort(stmts, 0, stmts.length, keyOrder.getComparator());
        }
        return stmts;
    }

    public void close() {
        src.close();
        if (sameAs3It != null)
            sameAs3It.close();
        if (sameAs2It != null)
            sameAs2It.close();
        sameAs3.closeAndDelete();
        sameAs2.closeAndDelete();
    }

    public void remove() {
        if (canRemove) {
            src.remove();
        }
    }
}
