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
 * Created on May 20, 2007
 */

package com.bigdata.rdf.store;

import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.Parser;
import org.openrdf.sesame.constants.RDFFormat;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.UnicodeKeyBuilder;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.model.OptimizedValueFactory._Statement;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.rio.LoadStats;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.spo.Justification;
import com.bigdata.rdf.spo.SPOBuffer;
import com.bigdata.rdf.util.KeyOrder;
import com.bigdata.rdf.util.RdfKeyBuilder;

/**
 * Interface for a triple store.
 * <p>
 * Note: This API does NOT implement a Truth Maintenance (TM) strategy and by
 * itself only supports "explicit" triples. If the knowledge base is NOT to
 * store any entailments then the application MAY directly use this API to read
 * and write explicit statements on the knowledge base. However, if the
 * knowledge base is to directly store any entailments, then the application
 * MUST NOT invoke operations on this API that add statements to, or remove
 * statements from, the knowledge base as the entailments will not be updated
 * properly.
 * <p>
 * When entailments are stored in the knowledge base, a TM strategy MUST be used
 * to made those entailments based on the explicit triples asserted or retracted
 * by the application. When an application requests that statements are added to
 * a knowledge base that maintains entailments, the TM strategy MAY need to add
 * additional entailments to the knowledge base. When an application requests
 * that statement(s) are removed from the knowledge base, the TM strategy needs
 * to consider the state of the knowledge base. In general, a statement should
 * be removed IFF it was {@link StatementEnum#Explicit} AND the statement is no
 * longer entailed by the model theory and the remaining statements in the
 * knowledge base.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ITripleStore {

    final public Logger log = Logger.getLogger(ITripleStore.class);

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

    /*
     * Note: access to the indices through these methods is required to support
     * partitioned indices since the latter introduce an indirection to: (a)
     * periodically replace a mutable btree with a fused view of the btree on
     * the journal and one or more index segments; (b) periodically split or
     * join partitions of the index based on the size on disk of that partition.
     * 
     * At this time is is valid to hold onto a reference during a given load or
     * query operation but not across commits (more accurately, not across
     * overflow() events). Eventually we will have to put a client api into
     * place that hides the routing of btree api operations to the journals and
     * segments for each index partition.
     */
    public IIndex getTermIdIndex();

    public IIndex getIdTermIndex();

    public IIndex getSPOIndex();

    public IIndex getPOSIndex();

    public IIndex getOSPIndex();

    /**
     * Return the statement index identified by the {@link KeyOrder}.
     * 
     * @param keyOrder The key order.
     * 
     * @return The statement index for that access path.
     */
    public IIndex getStatementIndex(KeyOrder keyOrder);

    /**
     * The #of triples in the store.
     * <p>
     * This may be an estimate when using partitioned indices.
     */
    public int getStatementCount();

    /**
     * The #of terms in the store.
     * <p>
     * This may be an estimate when using partitioned indices.
     */
    public int getTermCount();

    /**
     * The #of URIs in the store.
     * <p>
     * This may be an estimate when using partitioned indices.
     */
    public int getURICount();

    /**
     * The #of Literals in the store.
     * <p>
     * This may be an estimate when using partitioned indices.
     */
    public int getLiteralCount();

    /**
     * The #of BNodes in the store.
     * <p>
     * This may be an estimate when using partitioned indices.
     */
    public int getBNodeCount();

    /**
     * Return the shared {@link RdfKeyBuilder} instance for this client. The
     * object will be compatible with the Unicode preferences that are in effect
     * for the {@link ITripleStore}.
     * <p>
     * Note: This object is NOT thread-safe.
     */
    public RdfKeyBuilder getKeyBuilder();
    
    /**
     * Add a single {@link StatementEnum#Explicit} statement by lookup and/or
     * insert into the various indices (non-batch api).
     * <p>
     * Note: The non-batch API is horridly inefficient. Whenever possible use
     * the batch API either directly or by means of {@link StatementBuffer} or
     * {@link SPOBuffer}.
     */
    public void addStatement(Resource s, URI p, Value o);

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

    /**
     * Return true if the statement exists in the store (non-batch API).
     * <p>
     * Note: This does not verify whether or not the statement is explicit.
     * 
     * @param s
     *            Optional subject.
     * @param p
     *            Optional predicate.
     * @param o
     *            Optional object.
     */
    public boolean containsStatement(Resource s, URI p, Value o);

    /**
     * Chooses and returns the best {@link IAccessPath} for the given triple
     * pattern.
     * 
     * @param s
     *            The term identifier for the subject -or-
     *            {@link ITripleStore#NULL}.
     * @param p
     *            The term identifier for the predicate -or-
     *            {@link ITripleStore#NULL}.
     * @param o
     *            The term identifier for the object -or-
     *            {@link ITripleStore#NULL}.
     */
    public IAccessPath getAccessPath(long s, long p, long o);

    /**
     * Return the {@link IAccessPath} for the specified {@link KeyOrder}.
     */
    public IAccessPath getAccessPath(KeyOrder keyOrder);
    
    /**
     * Unconditionally removes statement(s) matching the triple pattern (NO
     * truth maintenance).
     * 
     * @param s
     * @param p
     * @param o
     * 
     * @return The #of statements removed.
     */
    public int removeStatements(Resource s, URI p, Value o);

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
     * Adds the statements to each index (batch api, NO truth maintenance).
     * <p>
     * Pre-conditions: The term identifiers for each {@link _Statement} are
     * defined.
     * 
     * @param stmts
     *            An array of statements
     * 
     * @see #insertTerms(_Value[], int, boolean, boolean)
     * 
     * @return The #of statements written on the database.
     * 
     * @todo the best way to use this is by creating a {@link StatementBuffer},
     *       filling it up, and then flushing it to the database. Document that
     *       and perhaps get this of this method on the public API.
     */
    public int addStatements(_Statement[] stmts, int numStmts);

    /**
     * Generate the sort keys for the terms.
     * 
     * @param keyBuilder
     *            The object used to generate the sort keys - <em>this is not
     *            safe for concurrent writers</em>
     * @param terms
     *            The terms whose sort keys will be generated.
     * @param numTerms
     *            The #of terms in that array.
     * 
     * @see #createCollator()
     * @see UnicodeKeyBuilder
     */
    public void generateSortKeys(RdfKeyBuilder keyBuilder, _Value[] terms,
            int numTerms);

    /**
     * Batch insert of terms into the database.
     * 
     * @param terms
     *            An array whose elements [0:nterms-1] will be inserted.
     * @param numTerms
     *            The #of terms to insert.
     * @param haveKeys
     *            True if the terms already have their sort keys.
     * @param sorted
     *            True if the terms are already sorted by their sort keys (in
     *            the correct order for a batch insert).
     * 
     * @exception IllegalArgumentException
     *                if <code>!haveKeys && sorted</code>.
     */
    public void insertTerms(_Value[] terms, int numTerms, boolean haveKeys,
            boolean sorted);

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
     * 
     * @todo Add getTerm(s,p,o) returning _Value[] to batch each statement
     *       lookup so that we do less RPCs in a distributed system.
     */
    public _Value getTerm(long id);

    /**
     * Return the pre-assigned termId for the value (non-batch API).
     * 
     * @param value
     *            Any {@link Value} reference (MAY be <code>null</code>).
     * 
     * @return The pre-assigned termId -or- {@link #NULL} iff the term is not
     *         known to the database.
     */
    public long getTermId(Value value);

    /**
     * Load data into the triple store (NO truth maintenance).
     * 
     * @param resource
     *            The resource -or- file.
     *            <p>
     *            Note: To refer to a resource in a package somewhere on the
     *            CLASSPATH write the name of the resource like this:
     *            <code>/com/bigdata/rdf/inf/testClosure01.nt</code>. The
     *            leading slash is important.
     * @param baseURI
     *            The baseURI or <code>""</code> if none is known.
     * @param rdfFormat
     *            The RDF interchange syntax to be parsed.
     * @param verifyData
     *            Controls the {@link Parser#setVerifyData(boolean)} option.
     * @param commit
     *            A {@link #commit()} will be performed IFF true.
     * 
     * @return Statistics about the data load operation.
     * 
     * @throws IOException
     *             if there is a problem when parsing the data.
     */
    public LoadStats loadData(String resource, String baseURI, RDFFormat rdfFormat,
            boolean verifyData, boolean commit) throws IOException;

    /**
     * Utility method dumps the statements in the store onto {@link System#err}
     * using the SPO index (subject order).
     */
    public void dumpStore();

    /**
     * Writes out some usage details on System.err.
     */
    public void usage();
    
    /**
     * A read-only copy of the properties used to configure the
     * {@link ITripleStore}.
     */
    public Properties getProperties();
 
    /**
     * Commit changes on the database.
     * <p>
     * Note: The semantics of this operation depend on whether the database is
     * embedded (does a commit), temporary (ignored), or a federation (ignored).
     */
    public void commit();
    
    /**
     * Clear all data.
     */
    public void clear();
    
    /**
     * Close the client. If the client uses an embedded database, then close the
     * embedded database as well.
     */
    public void close();
    
    /**
     * Close the client. If the client uses an embedded database, then close and
     * delete the embedded database as well.
     */
    public void closeAndDelete();
    
    /**
     * True iff the backing store is stable (exists on disk somewhere and may be
     * closed and re-opened).
     */
    public boolean isStable();

}
