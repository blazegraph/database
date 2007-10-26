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

import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPOBuffer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IAccessPath;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.rdf.store.ITripleStore;
import com.bigdata.rdf.store.TempTripleStore;

/**
 * A rule.
 * 
 * @todo Since a variable is just a negative long integer, it is possible to
 *       encode a predicate just like we do a statement with an added flag
 *       either before or after the s:p:o keys indicating whether the predicate
 *       is a magic/1 or a triple/3. This means that we can store predicates
 *       directly in the database in a manner that is consistent with triples.
 *       Since we can store predicates directly in the database we can also
 *       store rules simply be adding the body of the rule as the value
 *       associated with the predicate key. This will let us combine the rule
 *       base, the magic predicates, and the answer set together. We can then
 *       use the existing btree operations for access. The answer set can be
 *       extracted by filtering out the rules. (We should probably partition the
 *       keys so that rules are always in a disjoint part of the key space,
 *       e.g., by including a leading byte that is 0 for a triple and 1 for a
 *       rule.)
 *       <P>
 *       I don't think that we will be storing rules and magic facts in the
 *       database so this trick probably does not matter.
 *       <p>
 *       Also, variable identifiers are strictly local to a rule, not global to
 *       the database.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class Rule {

    final public Logger log = Logger.getLogger(Rule.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final public boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final public boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    /**
     * The persistent database.
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
    static final protected long NULL = ITripleStore.NULL;
    
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

    /**
     * Map N executions of rule over the terms in the tail. In each pass term[i]
     * will read from <i>tmp</i> and the other term(s) will read from a fused
     * view of <i>tmp</i> and the {@link #db}.
     * <p>
     * Within each pass, the decision on the order in which the terms will be
     * evaluated is made based on the rangeCount() for the {@link IAccessPath}
     * associated with the triple pattern for that term.
     * <p>
     * The N passes themselves are executed concurrently.
     * 
     * @param stats
     *            Statistics on each rule application will be appended to this
     *            list.
     * @param tmp
     *            An {@link AbstractTripleStore} containing data to be added to
     *            (or removed from) the {@link #db}.
     * @param buffer
     *            The rules will write the entailments (and optionally the
     *            justifications) on the buffer. The caller can read the
     * 
     * @return An iterator that may be used to read union of the entailments
     *         licensed by each pass over the rule.
     * 
     * @todo Whether or not to compute the justifications is an apply() time
     *       parameter, not a constant fixed when the rule is created. We only
     *       need the justifications when we are adding statements to the
     *       database. When we are removing statements they should not be
     *       computed.
     *       <p>
     *       In order to get out the justifications the caller should just pass
     *       in a JustificationBuffer.
     * 
     * @todo Make the {@link SPOBuffer} thread-safe so that the N passes may be
     *       concurrent and they all write onto the same buffer, hence their
     *       union is automatically visible in the iterator wrapping that
     *       buffer.
     * 
     * @todo In order to use a reader/write (or pipe) model we need special
     *       {@link ISPOIterator} (and IJustificationIterator) implementations
     *       that wrap the appropriate buffer, e.g.,
     * 
     * <pre>
     *       SPOBuffer buffer = new SPOBuffer(...);
     *       
     *       ISPOIterator itr = new SPOPipeIterator( buffer );
     *       
     *       rule.apply(..., buffer );
     *       
     *       while(itr.hasNext()) {
     *       
     *        SPO[] stmts = itr.nextChunk();
     *       
     *       }
     *       
     * </pre>
     * 
     * before calling apply. The caller then has a handle on the object from
     * which they can read the entailments and do whatever they like with them.
     * <P>
     * Note: The SPOBuffer would require a means to handshake with the iterator
     * so that it could signal when no more data will be made available to the
     * iterator.
     * <p>
     * For example, you can use
     * {@link IRawTripleStore#addStatements(ISPOIterator, com.bigdata.rdf.spo.ISPOFilter)}
     * if you want to jam the results into a database. If you just want to scan
     * the results, then you can do that directly using the iterator.
     * <p>
     * Normally you would want to consume the iterator in chunks, sorting each
     * chunk into the natural order for some index operation and then doing that
     * operation on each chunk in turn.
     * 
     * @todo We can in fact run the variations of the rule in parallel using an
     *       {@link ExecutorService}. The {@link InferenceEngine} should
     *       declare this service. The service will be used for both map
     *       parallelism and for parallelism of subqueries within rules.
     */
    public ISPOIterator apply( List<RuleStats> stats, AbstractTripleStore tmp, SPOBuffer buffer ) {

        throw new UnsupportedOperationException();
        
    }
    
}
