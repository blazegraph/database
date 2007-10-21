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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.Parser;
import org.openrdf.sesame.constants.RDFFormat;

import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.UnicodeKeyBuilder;
import com.bigdata.rdf.model.OptimizedValueFactory._Statement;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.rio.LoadStats;
import com.bigdata.rdf.util.KeyOrder;
import com.bigdata.rdf.util.RdfKeyBuilder;

/**
 * Interface for a triple store.
 * 
 * @todo lucene integration.
 * 
 * @todo write quad store (Sesame 2.x).
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

    static final public String name_termId = "terms";
    static final public String name_idTerm = "ids";

    static final public String name_spo = KeyOrder.SPO.name;
    static final public String name_pos = KeyOrder.POS.name;
    static final public String name_osp = KeyOrder.OSP.name;

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
     * Note: This object is NOT thread-safe. When multiple threads are used,
     * each thread should be a different client.
     */
    public RdfKeyBuilder getKeyBuilder();
    
    /**
     * Add a single statement by lookup and/or insert into the various indices
     * (non-batch api).
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
     * Return a range query iterator that will visit the statements matching the
     * triple pattern using the best access path given the triple pattern.
     * 
     * @param s
     *            An optional term identifier for the subject role or
     *            {@link #NULL}.
     * @param p
     *            An optional term identifier for the predicate role or
     *            {@link #NULL}.
     * @param o
     *            An optional term identifier for the object role or
     *            {@link #NULL}.
     * 
     * @return The range query iterator.
     */
    public IEntryIterator rangeQuery(long s, long p, long o);

    /**
     * Return the #of statements matching the triple pattern using the best
     * access path given the triple pattern (the count will be approximate if
     * partitioned indices are being used).
     * 
     * @param s
     *            An optional term identifier for the subject role or
     *            {@link #NULL}.
     * @param p
     *            An optional term identifier for the predicate role or
     *            {@link #NULL}.
     * @param o
     *            An optional term identifier for the object role or
     *            {@link #NULL}.
     * 
     * @return The range count.
     */
    public int rangeCount(long s, long p, long o);

    /**
     * Removes statements matching the triple pattern.
     * 
     * @param s
     * @param p
     * @param o
     * 
     * @return The #of statements removed.
     */
    public int removeStatements(Resource s, URI p, Value o);

    /**
     * Remove statements matching the triple pattern.
     * 
     * @param _s
     * @param _p
     * @param _o
     * 
     * @return The #of statements removed.
     */
    public int removeStatements(long _s, long _p, long _o);

    /**
     * Value used for a "NULL" term identifier.
     * 
     * @todo use this throughout rather than "0" since the value should really
     *       be an <em>unsigned long</em>).
     */
    public static final long NULL = 0L;

    /**
     * Adds the statements to each index (batch api).
     * <p>
     * Note: this is not sorting by the generated keys so the sort order may not
     * perfectly reflect the natural order of the index. however, i suspect that
     * it simply creates a few partitions out of the natural index order based
     * on the difference between signed and unsigned interpretations of the
     * termIds when logically combined into a statement identifier.
     * 
     * @param stmts
     *            An array of statements
     */
    public void addStatements(_Statement[] stmts, int numStmts);

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
     */
    public _Value getTerm(long id);

    /**
     * Return the pre-assigned termId for the value (non-batch API).
     * 
     * @param value
     *            The value.
     * 
     * @return The pre-assigned termId -or- {@link #NULL} iff the term is not
     *         known to the database.
     */
    public long getTermId(Value value);

    /**
     * Load data into the triple store.
     * 
     * @param file
     *            The file.
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
    public LoadStats loadData(File file, String baseURI, RDFFormat rdfFormat,
            boolean verifyData, boolean commit) throws IOException;

    /**
     * Performs an efficient scan of a statement index returning the distinct
     * term identifiers found in the first key component for the named access
     * path. Depending on which access path you are using, this will be the term
     * identifiers for the distinct subjects, predicates, or values in the KB.
     * 
     * @param keyOrder
     *            Names the access path. Use {@link KeyOrder#SPO} to get the
     *            term identifiers for the distinct subjects,
     *            {@link KeyOrder#POS} to get the term identifiers for the
     *            distinct predicates, and {@link KeyOrder#OSP} to get the term
     *            identifiers for the distinct objects
     * 
     * @return The distinct term identifiers in the first key slot for the
     *         triples in that index.
     */
    public ArrayList<Long> distinctTermScan(KeyOrder keyOrder);

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
     * 
     * @todo change semantics to immediate close/disconnect.  Add shutdown() and
     * shutdownNow()?
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
