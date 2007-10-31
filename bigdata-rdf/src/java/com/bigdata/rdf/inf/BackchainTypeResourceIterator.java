/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Oct 30, 2007
 */

package com.bigdata.rdf.inf;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.util.KeyOrder;

/**
 * Provides backward chaining for (x rdf:type rdfs:Resource).
 * <p>
 * Note: You only need to do this on read from a high level query language since
 * the rest of the RDFS rules will run correctly without the (x rdf:type
 * rdfs:Resource) entailments being present. Further, you only need to do this
 * when the {@link InferenceEngine} was instructed to NOT store the (x rdf:type
 * rdfs:Resource) entailments.
 * 
 * @todo The iterator as written will deliver a redundent (x rdf:type
 *       rdfs:Resource) entailment iff there is an explicit statement (x
 *       rdf:type rdfs:Resource) in the database. The "right" way to fix that is
 *       to notice all such explicit statements as we deliver the underlying
 *       iterator to the caller and then NOT generate the corresponding
 *       entailment in the iterator. However, that practice can not scan if
 *       people dump a large number of such explicit statements into the store.
 *       <p>
 *       The alternative is to filter all explicit (x rdf:type rdfs:Resource)
 *       statements from the underlying iterator and deliver them only as
 *       entailments. This means that the caller will always see these as
 *       "Inferred" and never as "Explicit".  If we do this, then we also 
 *       need to do more work to keep the iterator "chunked".
 * 
 * @see InferenceEngine
 * @see InferenceEngine.Options
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */ 
public class BackchainTypeResourceIterator implements ISPOIterator {
    
    private final ISPOIterator src;
//    private final long s, p, o;
//    private final AbstractTripleStore db;
    private final long rdfType, rdfsResource;
    private final KeyOrder keyOrder;

    /**
     * The subject(s) whose (s rdf:type rdfs:Resource) entailments will be
     * visited.
     */
    private Iterator<Long> subjects;
    
    private boolean sourceExhausted = false;
    
    private boolean open = true;

//    private void assertOpen() {
//        
//        if (!open)
//            throw new IllegalStateException();
//        
//    }
    
    /**
     * Create an iterator that will visit all statements in the source iterator
     * and also backchain any entailments of the form (x rdf:type rdfs:Resource)
     * which are valid for the given triple pattern.
     * 
     * @param src
     *            The source iterator.
     * @param s
     *            The subject of the triple pattern.
     * @param p
     *            The predicate of the triple pattern.
     * @param o
     *            The object of the triple pattern.
     * @param db
     *            The database from which we will read the distinct subject
     *            identifiers (iff this is an all unbound triple pattern).
     * @param rdfType
     *            The term identifier that corresponds to rdf:Type for the
     *            database.
     * @param rdfsResource
     *            The term identifier that corresponds to rdf:Resource for the
     *            database.
     */
    public BackchainTypeResourceIterator(ISPOIterator src, long s, long p,
            long o, AbstractTripleStore db, long rdfType, long rdfsResource) {
        
        this.src = src;
        
        this.keyOrder = src.getKeyOrder(); // MAY be null.

//        this.s = s;
//        
//        this.p = p;
//        
//        this.o = o;
        
        if (s == NULL && p == NULL && o == NULL) {

            /*
             * Backchain will generate one statement for each distinct subject
             * in the store.
             */

            subjects = db.getAccessPath(KeyOrder.SPO).distinctTermScan();

        } else if (p == NULL && o == NULL) {

            /*
             * Backchain will generate exactly one statement: (s rdf:type
             * rdfs:Resource).
             */

            subjects = Arrays.asList(new Long[] { s }).iterator();

        } else {

            /*
             * Backchain will not generate any statements.
             */

            subjects = Arrays.asList(new Long[] {}).iterator();

        }

        // this.db = db;
        
        this.rdfType = rdfType;
        
        this.rdfsResource = rdfsResource;
        
    }

    public KeyOrder getKeyOrder() {

        return keyOrder;
        
    }

    public void close() {

        if(!open) return;
        
        // release any resources here.
        
        open = false;

        src.close();

        subjects = null;
        
    }

    public boolean hasNext() {
        
        if (!open) {

            // the iterator has been closed.
            
            return false;
            
        }

        if (!sourceExhausted) {

            if (src.hasNext()) {

                // still consuming the source iterator.

                return true;

            }

            // the source iterator is now exhausted.

            sourceExhausted = true;

            src.close();

        }

        if (subjects.hasNext()) {

            // still consuming the subjects iterator.
            
            return true;
            
        }
        
        // the subjects iterator is also exhausted so we are done.
        
        return false;
        
    }

    public SPO next() {

        if (!hasNext())
            throw new NoSuchElementException();

        if(src.hasNext()) {
            
            return src.next();
            
        }
        
        return new SPO(subjects.next(), rdfType, rdfsResource,
                StatementEnum.Inferred);
    
    }

    public SPO[] nextChunk() {

        if (!hasNext())
            throw new NoSuchElementException();
        
        if(!sourceExhausted) {
            
            return src.nextChunk();
            
        }

        /*
         * Create a "chunk" of entailments.
         */
        
        final int chunkSize = 1000;
        
        long[] s = new long[chunkSize];
        
        int n = 0;
        
        while(subjects.hasNext() && n < s.length ) {
            
            s[n++] = subjects.next();
            
        }
        
        SPO[] stmts = new SPO[n];
        
        for(int i=0; i<n; i++) {
            
            stmts[i] = new SPO(s[i], rdfType, rdfsResource,
                    StatementEnum.Inferred);
            
        }
        
        return stmts;
        
    }

    public SPO[] nextChunk(KeyOrder keyOrder) {
        
        if (keyOrder == null)
            throw new IllegalArgumentException();

        SPO[] stmts = nextChunk();

        if (keyOrder != this.keyOrder) {

            // sort into the required order.

            Arrays.sort(stmts, 0, stmts.length, keyOrder.getComparator());

        }

        return stmts;
        
    }

    /**
     * Note: You can not "remove" the backchained entailments. The only possible
     * semantics for this would be to remove an explicit entailment returned by
     * the underlying iterator. Those semantics could be implemented.
     * 
     * @throws UnsupportedOperationException
     *             always.
     */
    public void remove() {

        throw new UnsupportedOperationException();
        
    }
    
}
