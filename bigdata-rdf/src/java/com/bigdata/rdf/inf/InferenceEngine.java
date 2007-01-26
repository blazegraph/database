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

import java.io.IOException;
import java.util.Properties;

import org.openrdf.model.impl.URIImpl;
import org.openrdf.vocabulary.RDF;
import org.openrdf.vocabulary.RDFS;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Options;
import com.bigdata.objndx.IEntryIterator;
import com.bigdata.rdf.StatementIndex;
import com.bigdata.rdf.TermIndex;
import com.bigdata.rdf.TripleStore;
import com.bigdata.rdf.inf.TestMagicSets.MagicRule;

/**
 * Adds support for RDFS inference.
  * <p>
 * A fact always has the form:
 * 
 * <pre>
 * triple(s, p, o)
 * </pre>
 * 
 * where s, p, and or are identifiers for RDF values in the {@link TermIndex}.
 * Facts are stored either in the long-term database or in a per-query answer
 * set.
 * <p>
 * A rule always has the form:
 * 
 * <pre>
 *   pred :- pred*.
 * </pre>
 * 
 * where <i>pred</i> is either
 * <code>magic(triple(varOrId,varOrId,varOrId))</code> or
 * <code>triple(varOrId,varOrId,varOrId)</code>. A rule is a clause
 * consisting of a head (a predicate) and a body (one or more predicates). Note
 * that the body of the rule MAY be empty. When there are multiple predicates in
 * the body of a rule the rule succeeds iff all predicates in the body succeed.
 * When a rule succeeds, the head of the clause is asserted. If the head is a
 * predicate then it is asserted into the rule base for the query. If it is a
 * fact, then it is asserted into the database for the query. Each predicate has
 * an "arity" with is the number of arguments, e.g., the predicate "triple" has
 * an arity of 3 and may be written as triple/3 while the predicate "magic" has
 * an arity of 1 and may be written as magic/1.
 * <p>
 * A copy is made of the basic rule base at the start of each query and a magic
 * transform is applied to the rule base, producing a new rule base that is
 * specific to the query. Each query is also associated with an answer set in
 * which facts are accumulated. Query execution consists of iteratively applying
 * all rules in the rule base. Execution terminates when no new facts or rules
 * are asserted in a given iteration - this is the <i>fixed point</i> of the
 * query.
 * <p>
 * Note: it is also possible to run the rule set without creating a magic
 * transform. This will produce the full forward closure of the entailments.
 * This is done by using the statements loaded from some source as the source
 * fact base and inserting the entailments created by the rules back into
 * statement collection. When the rules reach their fixed point, the answer set
 * contains both the told triples and the inferred triples and is simply
 * inserted into the long-term database.
 * <p>
 * rdfs9 is represented as:
 * 
 * <pre>
 *    triple(?v,rdf:type,?x) :-
 *       triple(?u,rdfs:subClassOf,?x),
 *       triple(?v,rdf:type,?u). 
 * </pre>
 * 
 * rdfs11 is represented as:
 * 
 * <pre>
 *    triple(?u,rdfs:subClassOf,?x) :-
 *       triple(?u,rdfs:subClassOf,?v),
 *       triple(?v,rdf:subClassOf,?x). 
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class InferenceEngine extends TripleStore {

    /**
     * Used to assign unique variable identifiers.
     */
    private long nextVar = -1;

    /**
     * Return the next unique variable identifier.
     */
    protected Var nextVar() {

        return new Var(nextVar--);

    }

    /*
     * Identifiers for well-known RDF values. 
     */
    Id rdfType;
    Id rdfsSubClassOf;

    /*
     * Rules.
     */
    Rule rdfs9;
    Rule rdfs11;

    /**
     * All rules defined by the inference engine.
     */
    Rule[] rules;

    /**
     * @param properties
     * @throws IOException
     */
    public InferenceEngine(Properties properties) throws IOException {

        super(properties);

        setup();

    }

    /**
     * Sets up the inference engine.
     */
    protected void setup() {

        setupIds();

        setupRules();

    }

    /**
     * Resolves or defines well-known RDF values.
     * 
     * @see #rdfType and friends which are initialized by this method.
     */
    protected void setupIds() {

        rdfType = new Id(addTerm(new URIImpl(RDF.TYPE)));

        rdfsSubClassOf = new Id(addTerm(new URIImpl(RDFS.SUBCLASSOF)));

    }

    public void setupRules() {

        rdfs9 = new RuleRdfs9(this, nextVar(), nextVar(), nextVar());

        rdfs11 = new RuleRdfs11(this, nextVar(), nextVar(), nextVar());

        rules = new Rule[] { rdfs9, rdfs11 };

    }

    /**
     * Compute the complete forward closure of the store using a
     * set-at-a-time inference strategy.
     * 
     * @todo refactor so that we can close a document that we are loading
     *       before it is inserted into the main database.
     * 
     * @todo can the dependency array among the rules be of use to us when
     *       we are computing full foward closure as opposed to using magic
     *       sets to answer a specific query?
     */
    public void fullForwardClosure() {

        final Rule[] rules = this.rules;

        final int nrules = rules.length;

        int lastStatementCount = ndx_spo.getEntryCount();

        final long begin = System.currentTimeMillis();

        System.err.println("Closing kb with " + lastStatementCount
                + " statements");

        int nadded = 0;

        while (true) {

            for (int i = 0; i < nrules; i++) {

                Rule rule = rules[i];

                nadded += rule.apply();

            }

            int statementCount = ndx_spo.getEntryCount();

            // testing the #of statement is less prone to error.
            if (lastStatementCount == statementCount) {

                //                if( nadded == 0 ) { // should also work.

                // This is the fixed point.
                break;

            }

        }

        final long elapsed = System.currentTimeMillis() - begin;

        System.err.println("Closed store in " + elapsed + "ms yeilding "
                + lastStatementCount + " statements total");

    }

    /**
     * Extracts the object ids from a key scan.
     * 
     * @param itr
     *            The key scan iterator.
     *            
     * @return The objects visited by that iterator.
     */
    public SPO[] getStatements(StatementIndex ndx, byte[] fromKey, byte[] toKey) {

        final int n = ndx.rangeCount(fromKey, toKey);

        // buffer for storing the extracted s:p:o data.
        SPO[] ids = new SPO[n];

        IEntryIterator itr1 = ndx.rangeIterator(fromKey, toKey);

        int i = 0;

        while (itr1.hasNext()) {

            ids[i++] = new SPO(ndx.keyOrder,keyBuilder,itr1.getKey());

        }

        assert i == n;

        return ids;

    }

    /**
     * Accepts a triple pattern and returns the closure over that triple pattern
     * using a magic transform of the RDFS entailment rules.
     * 
     * @param query
     *            The triple pattern.
     * 
     * @param rules
     *            The rules to be applied.
     * 
     * @return The answer set.
     * 
     * @exception IllegalArgumentException
     *                if query is null.
     * @exception IllegalArgumentException
     *                if query is a fact (no variables).
     */
    public TripleStore query(Triple query, Rule[] rules) throws IOException {

        if (query == null)
            throw new IllegalArgumentException("query is null");

        if (query.isFact())
            throw new IllegalArgumentException("no variables");

        if (rules == null)
            throw new IllegalArgumentException("rules is null");

        if (rules.length == 0)
            throw new IllegalArgumentException("no rules");
        
        final int nrules = rules.length;

        /*
         * prepare the magic transform of the provided rules.
         */
        
        Rule[] rules2 = new Rule[nrules];
        
        for( int i=0; i<nrules; i++ ) {

            rules2[i] = new MagicRule(this,rules[i]);

        }
        
        /*
         * @todo create the magic seed and insert it into the answer set.
         */
        Magic magicSeed = new Magic(query);

        /*
         * Run the magic transform.
         */

        Properties answerSetProperties = new Properties();
        
        /*
         * @todo support buffer extension for the transient mode or set the
         * default capacity to something larger.  if things get too large
         * then we need to spill over to disk.
         */
        answerSetProperties.setProperty(Options.BUFFER_MODE,
                BufferMode.Transient.toString());
        
        TripleStore answerSet = new TripleStore(answerSetProperties);
        
        int lastStatementCount = ndx_spo.getEntryCount();

        final long begin = System.currentTimeMillis();

        System.err.println("Running query: "+query);

        int nadded = 0;

        while (true) {

            for (int i = 0; i < nrules; i++) {

                Rule rule = rules[i];

                nadded += rule.apply();

            }

            int statementCount = ndx_spo.getEntryCount();

            // testing the #of statement is less prone to error.
            if (lastStatementCount == statementCount) {

                //                if( nadded == 0 ) { // should also work.

                // This is the fixed point.
                break;

            }

        }

        final long elapsed = System.currentTimeMillis() - begin;

        System.err.println("Ran query in " + elapsed + "ms; "
                + lastStatementCount + " statements in answer set.");

        return answerSet;
        
    }

}
