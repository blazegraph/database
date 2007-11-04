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
 * Created on Oct 20, 2007
 */

package com.bigdata.rdf.store;

import org.openrdf.model.Value;

import com.bigdata.btree.IIndex;
import com.bigdata.rdf.inf.Justification;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.ISPOFilter;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.util.KeyOrder;

/**
 * Low-level API directly using long term identifiers rather than an RDF Value
 * object model.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IRawTripleStore extends ITripleStore {
    
    /**
     * Value used for a "NULL" term identifier.
     */
    public static final long NULL = 0L;
    
    /**
     * The #of terms in a statement (3 is a triple store, 4 is a quad store).
     * 
     * @todo use this constant throughout and then change over to a quad store.
     */
    public static final int N = 3;

    /**
     * The name of the index mapping terms to term identifiers.
     */
    static final public String name_termId = "terms";
    
    /**
     * The name of the index mapping term identifiers to terms.
     */
    static final public String name_idTerm = "ids";

    /*
     * The names of the various statement indices. 
     */
    
    static final public String name_spo = KeyOrder.SPO.toString();
    static final public String name_pos = KeyOrder.POS.toString();
    static final public String name_osp = KeyOrder.OSP.toString();
    
    /**
     * The name of the optional index in which {@link Justification}s are
     * stored.
     */
    static final public String name_just = "just";

    abstract public IIndex getTermIdIndex();

    abstract public IIndex getIdTermIndex();

    abstract public IIndex getSPOIndex();

    abstract public IIndex getPOSIndex();

    abstract public IIndex getOSPIndex();

    /**
     * Return the statement index identified by the {@link KeyOrder}.
     * 
     * @param keyOrder The key order.
     * 
     * @return The statement index for that access path.
     */
    abstract public IIndex getStatementIndex(KeyOrder keyOrder);

    /**
     * Add a term into the term:id index and the id:term index, returning the
     * assigned term identifier (non-batch API).
     * 
     * @param value
     *            The term.
     * 
     * @return The assigned term identifier.
     */
    public long addTerm(Value value);

    /**
     * Return the RDF {@link Value} given a term identifier (non-batch api).
     * 
     * @return the RDF value or <code>null</code> if there is no term with
     *         that identifier in the index.
     */
    public Value getTerm(long id);

    /**
     * Return the pre-assigned termId for the value (non-batch API).
     * 
     * @param value
     *            Any {@link Value} reference (MAY be <code>null</code>).
     * 
     * @return The pre-assigned termId -or- {@link IRawTripleStore#NULL} iff the term is not
     *         known to the database.
     */
    public long getTermId(Value value);

    /**
     * Chooses and returns the best {@link IAccessPath} for the given triple
     * pattern.
     * 
     * @param s
     *            The term identifier for the subject -or-
     *            {@link IRawTripleStore#NULL}.
     * @param p
     *            The term identifier for the predicate -or-
     *            {@link IRawTripleStore#NULL}.
     * @param o
     *            The term identifier for the object -or-
     *            {@link IRawTripleStore#NULL}.
     */
    public IAccessPath getAccessPath(long s, long p, long o);

    /**
     * Return the {@link IAccessPath} for the specified {@link KeyOrder} and
     * a fully unbound triple pattern.  This is generally used only when you
     * want to perform a {@link IAccessPath#distinctTermScan()}.
     */
    public IAccessPath getAccessPath(KeyOrder keyOrder);
    
    /**
     * Return the statement from the database (fully bound s:p:o only).
     * <p>
     * Note: This may be used to examine the {@link StatementEnum}.
     * 
     * @param s
     *            The term identifier for the subject.
     * @param p
     *            The term identifier for the predicate.
     * @param o
     *            The term identifier for the object.
     * 
     * @return The {@link SPO} for that statement, including its
     *         {@link StatementEnum} -or- <code>null</code> iff the statement
     *         is not in the database.
     * 
     * @exception IllegalArgumentException
     *                if any of the arguments is {@link #NULL}.
     */
    public SPO getStatement(long s, long p, long o);
    
    /**
     * Writes the statements onto the statements indices (batch, parallel, NO
     * truth maintenance).
     * 
     * @param stmts
     *            The statements.
     * 
     * @param numStmts
     *            The #of entries in <i>stmts</i> that are valid.
     * 
     * @return The #of statements that were written on the indices (a statement
     *         that was previously an axiom or inferred and that is converted to
     *         an explicit statement by this method will be reported in this
     *         count as well as any statement that was not pre-existing in the
     *         database).
     */
    public int addStatements(SPO[] stmts, int numStmts );
    
    /**
     * Writes the statements onto the statement indices (batch, parallel, NO
     * truth maintenance).
     * 
     * @param stmts
     *            The statements.
     * 
     * @param numStmts
     *            The #of entries in <i>stmts</i> that are valid.
     * 
     * @param filter
     *            Optional statement filter. Statements matching the filter are
     *            NOT added to the database.
     * 
     * @return The #of statements that were written on the indices (a statement
     *         that was previously an axiom or inferred and that is converted to
     *         an explicit statement by this method will be reported in this
     *         count as well as any statement that was not pre-existing in the
     *         database).
     */
    public int addStatements(SPO[] stmts, int numStmts, ISPOFilter filter );
    
    /**
     * Writes the statements onto the statement indices (batch, parallel, NO
     * truth maintenance).
     * 
     * @param stmts
     *            The statements.
     * 
     * @param numStmts
     *            The #of entries in <i>stmts</i> that are valid.
     * 
     * @param filter
     *            Optional statement filter. Statements matching the filter are
     *            NOT added to the database.
     * 
     * @return The #of statements that were written on the indices (a statement
     *         that was previously an axiom or inferred and that is converted to
     *         an explicit statement by this method will be reported in this
     *         count as well as any statement that was not pre-existing in the
     *         database).
     */
    public int addStatements(ISPOIterator itr, ISPOFilter filter);

    /**
     * Externalizes a statement using an appreviated syntax.
     */
    public String toString(long s, long p, long o);

    /**
     * Externalizes a term using an abbreviated syntax.
     * 
     * @param termId
     *            The term identifier.
     * 
     * @return A representation of the term.
     */
    public String toString(long termId);

}
