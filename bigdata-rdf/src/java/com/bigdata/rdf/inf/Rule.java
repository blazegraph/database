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
import com.bigdata.rdf.spo.SPOBuffer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.ITripleStore;
import com.bigdata.rdf.store.TempTripleStore;

/**
 * A rule.
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
     * The database against which queries are performed.
     */
    final protected AbstractTripleStore db;
    
    /**
     * The inference engine.
     */
    final protected InferenceEngine inf;
    
    /**
     * The head of the rule.
     */
    final protected Pred head;

    /**
     * The body of the rule -or- <code>null</code> if the body of the rule
     * is empty.
     */
    final protected Pred[] body;
    
    /**
     * The #of terms in the tail of the rule.
     */
    public int getTailCount() {
        
        return body.length;
        
    }

    /**
     * When true the rule must justify the entailments to
     * {@link SPOBuffer#add(com.bigdata.rdf.spo.SPO, com.bigdata.rdf.spo.Justification)}
     */
    final protected boolean justify;
    
    /**
     * The 64-bit long integer that represents an unassigned term identifier
     */
    final protected long NULL = ITripleStore.NULL;
    
    public Rule(InferenceEngine inf, Pred head, Pred[] body) {

        assert inf != null;
        
        assert head != null;

        this.db = inf.database;
        
        this.inf = inf;
        
        this.head = head;

        this.body = body;
        
        this.justify = inf.isJustified();

    }
    
    /**
     * Apply the rule, creating entailments that are inserted into a
     * {@link TempTripleStore}.
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
     *            <p>
     *            Note: In general, a single buffer object is reused by a series
     *            of rules. When the buffer overflows, entailments are
     *            transfered enmass into the backing store.
     * 
     * @return The statistics object.
     */
    public abstract RuleStats apply( final RuleStats stats, final SPOBuffer buffer );

}
