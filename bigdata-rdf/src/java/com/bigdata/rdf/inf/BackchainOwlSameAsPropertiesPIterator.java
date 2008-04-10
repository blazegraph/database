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
import java.util.Properties;
import java.util.Set;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.TempTripleStore;
import com.bigdata.rdf.util.KeyOrder;

/**
 * Provides backward chaining for property collection and reverse property
 * collection on owl:sameAs for the ?P? and ??? access paths.
 * <p>
 * Note:
 * 
 * @see InferenceEngine
 * @see InferenceEngine.Options
 * 
 * @author <a href="mailto:mpersonick@users.sourceforge.net">Mike Personick</a>
 */
public class BackchainOwlSameAsPropertiesPIterator extends
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
     * @param p
     *            The predicate of the triple pattern. Can be null.
     * @param db
     *            The database from which we will read the distinct subject
     *            identifiers (iff this is an all unbound triple pattern).
     * @param sameAs
     *            The term identifier that corresponds to owl:sameAs for the
     *            database.
     */
    public BackchainOwlSameAsPropertiesPIterator(ISPOIterator src, long p,
            AbstractTripleStore db, final long sameAs) {
        super(src, db, sameAs);
        Properties props = db.getProperties();
        // do not store terms
        props.setProperty(AbstractTripleStore.Options.LEXICON, "false");
        // only store the SPO index
        props.setProperty(AbstractTripleStore.Options.ONE_ACCESS_PATH, "true");
        sameAs2and3 = new TempTripleStore(props,db);
    }

    public KeyOrder getKeyOrder() {
        return src.getKeyOrder();
    }

    public boolean hasNext() {
        if (src.hasNext()) {
            return true;
        } else if (sameAs2and3It == null) {
            sameAs2and3It = sameAs2and3.getAccessPath(KeyOrder.SPO).iterator();
        }
        return sameAs2and3It.hasNext();
    }

    public SPO next() {
        canRemove = false;
        SPO current = null;
        if (src.hasNext()) {
            current = src.next();
            processSameAs2and3(current);
            canRemove = true;
        } else {
            if (sameAs2and3It == null) {
                sameAs2and3It = sameAs2and3.getAccessPath(KeyOrder.SPO).iterator();
            }
            if (sameAs2and3It.hasNext()) {
                current = sameAs2and3It.next();
            }
        }
        return current;
    }

    /**
     * We are essentially taking a cross product of the two sets {s, ? sameAs s}
     * and {o, ? sameAs o}, and then inferring links between those {s,o} pairs.
     * We can do this because s and o are unbound in this access path. We are
     * simply using the connection that we know about (the bound p), to connect
     * up the {s,o} pairs. When p is unbound we are essentially visiting all
     * predicates in the db and then doing the same thing.
     * 
     * @param spo
     *            the spo being visited by the source iterator
     */
    private void processSameAs2and3(SPO spo) {
        // join:
        // ( s sameAs ?sameS ) x ( s p o ) x ( o sameAs ?sameO )
        // where ( s != sameS) and ( o != sameO ) and ( p != sameAs )
        // to produce ( ?sameS p ?sameO ), ( s p ?sameO ), and ( ?sameS p o )
        // all of which might be present in the source iterator already
        // ignore sameAs properties

        // use a buffer so that we can do a more efficient batch contains
        // to filter out existing statements
        int chunkSize = 10000;
        SPO[] spos = new SPO[chunkSize];
        int numSPOs = 0;
        // create a new link between {s,? sameAs s} X {o,? sameAs o} tuples
        // using the supplied p
        Set<Long> sAndSames = getSelfAndSames(spo.s);
        Set<Long> oAndSames = getSelfAndSames(spo.o);
        for (long s1 : sAndSames) {
            for (long o1 : oAndSames) {
                // do not add ( s sameAs s ) inferences
                if (spo.p == sameAs && s1 == o1) {
                    continue;
                }
                if (numSPOs == chunkSize) {
                    // flush the buffer
                    boolean present = false; // filter for not present
                    ISPOIterator absent = db.bulkFilterStatements(spos,
                            numSPOs, present);
                    db.addStatements(sameAs2and3,copyOnly,absent, null);
                    numSPOs = 0;
                }
                // grow the buffer
                spos[numSPOs++] = new SPO(s1, spo.p, o1,
                        StatementEnum.Inferred);
            }
        }
        // final flush of the buffer
        boolean present = false; // filter for not present
        ISPOIterator absent = db.bulkFilterStatements(spos, numSPOs, present);
        db.addStatements(sameAs2and3,copyOnly,absent, null);
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
