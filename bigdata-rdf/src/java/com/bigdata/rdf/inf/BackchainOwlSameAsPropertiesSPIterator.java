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
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.TempTripleStore;
import com.bigdata.rdf.util.KeyOrder;

/**
 * Provides backward chaining for property collection on owl:sameAs for the SP?
 * and S?? access paths.
 * <p>
 * Note: This is a simple properties query: we know s and we want to know all
 * {p,o} tuples that constitute the properties for s.
 * 
 * @see InferenceEngine
 * @see InferenceEngine.Options
 * 
 * @author <a href="mailto:mpersonick@users.sourceforge.net">Mike Personick</a>
 */
public class BackchainOwlSameAsPropertiesSPIterator extends
        BackchainOwlSameAsIterator {
    private ISPOIterator sameAs2It, sameAs3It;

    private TempTripleStore sameAs2, sameAs3;

    private boolean canRemove = false;

    /**
     * Create an iterator that will visit all statements in the source iterator
     * and also backchain any entailments that would have resulted from
     * owl:sameAs {2,3}.
     * 
     * @param src
     *            The source iterator. {@link #nextChunk()} will sort statements
     *            into the {@link KeyOrder} reported by this iterator (as long
     *            as the {@link KeyOrder} is non-<code>null</code>).
     * @param s
     *            The subject of the triple pattern. Cannot be null.
     * @param p
     *            The predicate of the triple pattern. Can be null.
     * @param db
     *            The database from which we will read the distinct subject
     *            identifiers (iff this is an all unbound triple pattern).
     * @param sameAs
     *            The term identifier that corresponds to owl:sameAs for the
     *            database.
     */
    public BackchainOwlSameAsPropertiesSPIterator(ISPOIterator src, long s,
            long p, AbstractTripleStore db, final long sameAs) {
        super(src, db, sameAs);
        Properties props = db.getProperties();
        // do not store terms
        props.setProperty(AbstractTripleStore.Options.LEXICON, "false");
        // only store the SPO index
        props.setProperty(AbstractTripleStore.Options.ONE_ACCESS_PATH, "true");
        sameAs2 = new TempTripleStore(props);
        sameAs3 = new TempTripleStore(props);
        /*
         * Collect up additional properties (p and o values) for the known s
         * value by examining the values which are owl:sameAs s. The p might or
         * might not be bound in this access path.
         */
        {
            // join:
            // ( s sameAs ?same ) x ( ?same p ?o )
            // to produce ( s p ?o )
            // which might be present in the source iterator already
            // use a buffer so that we can do a more efficient batch contains
            // to filter out existing statements
            int chunkSize = 10000;
            SPO[] spos = new SPO[chunkSize];
            int numSPOs = 0;
            // get all of s's sames
            Iterator<Long> samesIt = getSames(s).iterator();
            while (samesIt.hasNext()) {
                long same = samesIt.next();
                // attach all of the same's properties to s
                ISPOIterator propsIt = db.getAccessPath(same, p, NULL).iterator();
                while (propsIt.hasNext()) {
                    SPO prop = propsIt.next();
                    // do not add ( s sameAs s ) inferences
                    if (prop.p == sameAs && s == prop.o) {
                        continue;
                    }
                    // flush the buffer if necessary
                    if (numSPOs == chunkSize) {
                        boolean present = false; // filter for not present
                        ISPOIterator absent = db.bulkFilterStatements(spos,
                                numSPOs, present);
                        db.addStatements(sameAs2,copyOnly,absent, null);
                        numSPOs = 0;
                    }
                    // attach the p and o to the original s
                    spos[numSPOs++] = new SPO(s, prop.p, prop.o,
                            StatementEnum.Inferred);
                }
            }
            // final flush of the buffer
            boolean present = false; // filter for not present
            ISPOIterator absent = db.bulkFilterStatements(spos, numSPOs,
                    present);
            db.addStatements(sameAs2,copyOnly,absent, null);
        }
        sameAs2It = sameAs2.getAccessPath(KeyOrder.SPO).iterator();
    }

    public KeyOrder getKeyOrder() {
        return src.getKeyOrder();
    }

    public boolean hasNext() {
        if (src.hasNext() || sameAs2It.hasNext()) {
            return true;
        } else if (sameAs3It == null) {
            sameAs3It = sameAs3.getAccessPath(KeyOrder.SPO).iterator();
        }
        return sameAs3It.hasNext();
    }

    /**
     * First iterate the source iterator and then iterate the sameAs{2}
     * iterator, which was fully populated in the ctor. Along the way, collect
     * up the sameAs{3} inferences, which will then be iterated once the first
     * two iterators are complete.
     */
    public SPO next() {
        canRemove = false;
        SPO current = null;
        if (src.hasNext()) {
            current = src.next();
            processSameAs3(current);
            canRemove = true;
        } else if (sameAs2It.hasNext()) {
            current = sameAs2It.next();
            processSameAs3(current);
        } else {
            if (sameAs3It == null) {
                sameAs3It = sameAs3.getAccessPath(KeyOrder.SPO).iterator();
            }
            if (sameAs3It.hasNext()) {
                current = sameAs3It.next();
            }
        }
        return current;
    }

    /**
     * Find all the alternate s values for this SPO, which we need to do since o
     * is unbound in this access path.
     * 
     * @param spo
     *            the spo being visited by the source iterator or the sameAs{2}
     *            iterator
     */
    private void processSameAs3(SPO spo) {
        // join:
        // ( s p o ) x ( o sameAs ?same )
        // to produce ( s p ?same )
        // which might be present in the source iterator already
        // ignore sameAs properties
        // use a buffer so that we can do a more efficient batch contains
        // to filter out existing statements
        int chunkSize = 10000;
        SPO[] spos = new SPO[chunkSize];
        int numSPOs = 0;
        // get all of o's sames
        Iterator<Long> samesIt = getSames(spo.o).iterator();
        while (samesIt.hasNext()) {
            long same = samesIt.next();
            // do not add ( s sameAs s ) inferences
            if (spo.p == sameAs && spo.s == same) {
                continue;
            }
            // flush the buffer if necessary
            if (numSPOs == chunkSize) {
                boolean present = false; // filter for not present
                ISPOIterator absent = db.bulkFilterStatements(spos, numSPOs,
                        present);
                db.addStatements(sameAs3,copyOnly,absent, null);
                numSPOs = 0;
            }
            // attach the new o to the original s and p
            spos[numSPOs++] = new SPO(spo.s, spo.p, same,
                    StatementEnum.Inferred);
        }
        // final flush of the buffer
        boolean present = false; // filter for not present
        ISPOIterator absent = db.bulkFilterStatements(spos, numSPOs, present);
        db.addStatements(sameAs3,copyOnly,absent, null);
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

    public SPO[] nextChunk(KeyOrder keyOrder) {
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
        if (sameAs2It != null)
            sameAs2It.close();
        if (sameAs3It != null)
            sameAs3It.close();
        sameAs2.closeAndDelete();
        sameAs3.closeAndDelete();
    }

    public void remove() {
        if (canRemove) {
            src.remove();
        }
    }
}
