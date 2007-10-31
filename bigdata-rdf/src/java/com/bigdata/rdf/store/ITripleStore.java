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

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.Parser;
import org.openrdf.sesame.constants.RDFFormat;
import org.openrdf.sesame.sail.StatementIterator;

import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.rio.LoadStats;
import com.bigdata.rdf.spo.ISPOIterator;

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
 * @see IRawTripleStore
 * @see AbstractTripleStore
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
     * Add a single {@link StatementEnum#Explicit} statement by lookup and/or
     * insert into the various indices (non-batch api).
     * <p>
     * Note: The non-batch API is horridly inefficient. The batch load API for
     * Sesame {@link Value} objects is:
     * 
     * <pre>
     *  
     *  StatementBuffer buffer = new StatementBuffer(store, ...);
     *  
     *  buffer.add( s, p, o );
     *  ...
     *  
     *  buffer.flush();
     *  
     * </pre>
     */
    public void addStatement(Resource s, URI p, Value o);

    /**
     * Return true if the triple pattern matches any statements in the store
     * (non-batch API).
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
    public boolean hasStatement(Resource s, URI p, Value o);

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
     * Returns an {@link IAccessPath} given a triple pattern expressed using
     * Sesame {@link Value} objects.
     * 
     * @param s
     * @param p
     * @param o
     * 
     * @return The object that may be used to read efficiently on the indices
     *         for that triple pattern.
     * 
     * @see IAccessPath
     * @see #asStatementIterator(ISPOIterator)
     */
    public IAccessPath getAccessPath(Resource s, URI p, Value o);
    
    /**
     * Wraps an {@link ISPOIterator} as a {@link StatementIterator}.
     * 
     * @param src
     *            An {@link ISPOIterator}
     * 
     * @return The {@link StatementIterator}.
     * 
     * @see IAccessPath
     * @see #getAccessPath(Resource, URI, Value)
     */
    public StatementIterator asStatementIterator(ISPOIterator src);

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
     * Commit changes on the database.
     * <p>
     * Note: The semantics of this operation depend on whether the database is
     * embedded (does a commit), temporary (ignored), or a federation (ignored).
     */
    public void commit();
    
    /**
     * Deletes all data for the {@link ITripleStore}.
     */
    public void clear();
    
    /**
     * Close the {@link ITripleStore}. If the client uses an embedded database,
     * then close the embedded database as well. If the client is connected to a
     * remote database, then only the connection is closed.
     */
    public void close();
    
}
