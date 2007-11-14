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
 * Created on May 20, 2007
 */

package com.bigdata.rdf.store;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.sesame.sail.StatementIterator;

import com.bigdata.rdf.inf.InferenceEngine;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore.EmptyAccessPath;
import com.bigdata.rdf.store.DataLoader.Options;

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
     *         for that triple pattern. In the special case where any of the
     *         given {@link Value}s is <strong>not known</strong> to the
     *         database this method will return an {@link EmptyAccessPath}.
     * 
     * @see IAccessPath
     * @see #asStatementIterator(ISPOIterator)
     */
    public IAccessPath getAccessPath(Resource s, URI p, Value o);
    
    /**
     * Wraps an {@link ISPOIterator} as a {@link StatementIterator}.
     * <p>
     * Note: The object visited will be {@link StatementWithType}s.
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
     * Convert an internal {@link SPO} into a Sesame {@link Statement}.
     * <p>
     * Note: The object returned will be a {@link StatementWithType}
     * 
     * @param spo
     *            The {@link SPO}.
     * 
     * @return The Sesame {@link Statement} -or- <code>null</code>.
     */
    public Statement asStatement(SPO spo);
    
    /**
     * Return a {@link DataLoader} singleton configured using the properties
     * that were used to configure the database.
     * 
     * @see Options
     */
    public DataLoader getDataLoader();
    
    /**
     * Return an {@link InferenceEngine} singleton configured using the
     * properties that were used to configure the database.
     * <p>
     * Note: The first time this object is requested it will attempt to write
     * the axioms on the database.
     * 
     * @see Options
     */
    public InferenceEngine getInferenceEngine();
    
    /**
     * Discard the write set.
     * <p>
     * Note: The semantics of this operation depend on whether the database is
     * embedded (discards the write set), temporary (ignored since the store is
     * not restart safe), or a federation (ignored since unisolated writes on
     * the federation are atomic and auto-committed).
     */
    public void abort();
    
    /**
     * Commit changes on the database.
     * <p>
     * Note: The semantics of this operation depend on whether the database is
     * embedded (does a commit), temporary (ignored since the store is not
     * restart safe), or a federation (ignored since unisolated writes on the
     * federation are atomic and auto-committed).
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
