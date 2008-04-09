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
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.TempTripleStore;
import com.bigdata.rdf.util.KeyOrder;

/**
 * Provides backward chaining for property collection and reverse property
 * collection on owl:sameAs for the SPO and S?O access paths.
 * <p>
 * Note: This is a relatively straightforward access path because both values of
 * interest for owl:sameAs (s and o) are known up front. We can collect up all
 * the sames in the ctor and use links between those sames to infer new links
 * between the original s and o.
 * 
 * @see InferenceEngine
 * @see InferenceEngine.Options
 * 
 * @author <a href="mailto:mpersonick@users.sourceforge.net">Mike Personick</a>
 */
public class BackchainOwlSameAsPropertiesSPOIterator extends
        BackchainOwlSameAsIterator {
    private ISPOIterator sameAs2and3It;

    private TempTripleStore sameAs2and3;

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
     * @param o
     *            The object of the triple pattern. Cannot be null.
     * @param db
     *            The database from which we will read the distinct subject
     *            identifiers (iff this is an all unbound triple pattern).
     * @param sameAs
     *            The term identifier that corresponds to owl:sameAs for the
     *            database.
     */
    public BackchainOwlSameAsPropertiesSPOIterator(ISPOIterator src, long s,
            long p, long o, AbstractTripleStore db, final long sameAs) {
        super(src, db, sameAs);
        Properties props = db.getProperties();
        // do not store terms
        props.setProperty(AbstractTripleStore.Options.LEXICON, "false");
        // only store the SPO index
        props.setProperty(AbstractTripleStore.Options.ONE_ACCESS_PATH, "true");
        sameAs2and3 = new TempTripleStore(props);
        /*
         * We are essentially taking a cross product of the two sets {s, ?
         * sameAs s} and {o, ? sameAs o}, then doing a query for all the links
         * between the {s,o} tuples, and using those links to infer links
         * between the original s and o values. If p is bound we are simply
         * trying to prove the existence of a statement by examining what we
         * know about things that are the sameAs s and o.
         */
        {
            // join:
            // ( s sameAs ?sameS ) x ( s ?p o ) x ( o sameAs ?sameO )
            // to produce ( s ?p o )
            // all of which might be present in the source iterator already
            // use a buffer so that we can do a more efficient batch contains
            // to filter out existing statements
            int chunkSize = 10000;
            SPO[] spos = new SPO[chunkSize];
            int numSPOs = 0;
            // collect up the links between {s,? sameAs s} X {o,? sameAs o}
            Set<Long> sAndSames = getSelfAndSames(s);
            Set<Long> oAndSames = getSelfAndSames(o);
            for (long s1 : sAndSames) {
                for (long o1 : oAndSames) {
                    // get the links between this {s,o} tuple
                    ISPOIterator it = db.getAccessPath(s1, p, o1).iterator();
                    while (it.hasNext()) {
                        long p1 = it.next().p;
                        // do not add ( s sameAs s ) inferences
                        if (p1 == sameAs && s == o) {
                            continue;
                        }
                        if (numSPOs == chunkSize) {
                            boolean present = false; // filter for not present
                            ISPOIterator absent = db.bulkFilterStatements(spos,
                                    numSPOs, present);
                            db.addStatements(sameAs2and3,copyOnly,absent, null);
                            numSPOs = 0;
                        }
                        spos[numSPOs++] = new SPO(s, p1, o,
                                StatementEnum.Inferred);
                    }
                }
            }
            // final flush of the buffer
            boolean present = false; // filter for not present
            ISPOIterator absent = db.bulkFilterStatements(spos, numSPOs,
                    present);
            db.addStatements(sameAs2and3,copyOnly,absent, null);
        }
        sameAs2and3It = sameAs2and3.getAccessPath(KeyOrder.SPO).iterator();
    }

    public KeyOrder getKeyOrder() {
        return src.getKeyOrder();
    }

    public boolean hasNext() {
        return src.hasNext() || sameAs2and3It.hasNext();
    }

    public SPO next() {
        canRemove = false;
        SPO current = null;
        if (src.hasNext()) {
            current = src.next();
            canRemove = true;
        } else if (sameAs2and3It.hasNext()) {
            current = sameAs2and3It.next();
        }
        return current;
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
        if (sameAs2and3It != null)
            sameAs2and3It.close();
        sameAs2and3.closeAndDelete();
    }

    public void remove() {
        if (canRemove) {
            src.remove();
        }
    }
}
