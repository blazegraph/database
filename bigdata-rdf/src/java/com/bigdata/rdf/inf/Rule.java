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
package com.bigdata.rdf.inf;

import com.bigdata.btree.BTree;
import com.bigdata.rdf.TempTripleStore;
import com.bigdata.rdf.TripleStore;


/**
 * 
 * @todo Since a variable is just a negative long integer, it is possible to
 *       encode a predicate just like we do a statement with an added flag
 *       either before or after the s:p:o keys indicating whether the
 *       predicate is a magic/1 or a triple/3. This means that we can store
 *       predicates directly in the database in a manner that is consistent
 *       with triples. Since we can store predicates directly in the
 *       database we can also store rules simply be adding the body of the
 *       rule as the value associated with the predicate key. This will let
 *       us combine the rule base, the magic predicates, and the answer set
 *       together. We can then use the existing btree operations for access.
 *       The answer set can be extracted by filtering out the rules. (We
 *       should probably partition the keys so that rules are always in a
 *       disjoint part of the key space, e.g., by including a leading byte
 *       that is 0 for a triple and 1 for a rule.)
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class Rule {

    /**
     * The inference engine.
     */
    final public InferenceEngine store;
    
    /**
     * The head of the rule.
     */
    final public Pred head;

    /**
     * The body of the rule -or- <code>null</code> if the body of the rule
     * is empty.
     */
    final public Pred[] body;

    /**
     * The 64-bit long integer that represents an unassigned term identifier
     * 
     * @todo use this throughout rather than "0" since the value should really
     *       be an <em>unsigned long</em>.
     */
    final public long NULL = TripleStore.NULL;
    
    public Rule(InferenceEngine store, Pred head, Pred[] body) {

        assert store != null;
        
        assert head != null;

        this.store = store;
        
        this.head = head;

        this.body = body;

    }
    
    /**
     * Apply the rule, creating entailments that are inserted into the temporary
     * store.
     * <p>
     * Note: the {@link BTree} class is NOT safe for concurrent modification
     * under traversal so implementations of this method need to buffer the
     * statements that they will insert. See {@link SPOBuffer}
     * 
     * @param stats
     *            Returns statistics on the rule application as a side effect.
     * @param buffer
     *            Used to buffer entailments so that we can perform batch btree
     *            operations. Entailments are batch inserted into the backing
     *            store (for the buffer) when the buffer overflows.
     * 
     * @return The statistics object.
     * 
     * @todo support conditional insert in the btree so that we do not have to
     *       do a lookup/insert combination.
     * 
     * @todo we could store proofs in the value for a statement or in a proofs
     *       index. doing that efficiently would require a concatenation
     *       operation variant for insert.
     * 
     */
    public abstract Stats apply( final Stats stats, final SPOBuffer buffer );
    
    /**
     * Statistics about what the Rule did during {@link Rule#apply()}.
     * 
     * @author mikep
     */
    public static class Stats {

        /**
         * #of matches for the triple pattern for the first antecedent of the rule.
         */
        public int stmts1;

        /**
         * #of matches for the triple pattern for the second antecedent of the
         * rule (if there are two).
         */
        public int stmts2;
        
        /**
         * If the rule performs a JOIN, this is the #of distinct queries that
         * are made for the 2nd triple pattern.  For some rules, we can reorder
         * the results from the first triple pattern in order to reduce the #of
         * subqueries.
         */
        public int numSubqueries;
        
        /**
         * #of statements considered.
         */
        public int numConsidered;
        
        /**
         * #of entailments computed.
         */
        public int numComputed;
        
        /**
         * Time to compute the entailments and store them within the
         * {@link TempTripleStore} in milliseconds.
         */
        long computeTime;
        
    }

}
