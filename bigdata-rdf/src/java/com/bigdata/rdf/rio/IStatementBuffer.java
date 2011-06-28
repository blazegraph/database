/*

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
 * Created on Nov 2, 2007
 */

package com.bigdata.rdf.rio;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.accesspath.IBuffer;

/**
 * Abstraction for a buffer that loads {@link Statement}s into an
 * {@link AbstractTripleStore}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IStatementBuffer<F extends Statement> extends IBuffer<F> {

    /**
     * The optional store into which statements will be inserted when non-<code>null</code>.
     */
    public AbstractTripleStore getStatementStore();

    /**
     * The database that will be used to resolve terms. When
     * {@link #getStatementStore()} is <code>null</code>, statements will be
     * written into this store as well.
     */
    public AbstractTripleStore getDatabase();

    /**
     * Add a statement to the buffer.
     * 
     * @param stmt
     *            The statement. If <i>stmt</i> implements
     *            {@link BigdataStatement} then the {@link StatementEnum} will
     *            be used (this makes it possible to load axioms into the
     *            database as axioms) but the term identifiers on the <i>stmt</i>'s
     *            values will be ignored.
     */
    public void add(F stmt);

    /**
     * Add an "explicit" statement to the buffer with a "null" context.
     * 
     * @param s
     *            The subject.
     * @param p
     *            The predicate.
     * @param o
     *            The object.
     */
    public void add(Resource s, URI p, Value o);

    /**
     * Add an "explicit" statement to the buffer.
     * 
     * @param s
     *            The subject.
     * @param p
     *            The predicate.
     * @param o
     *            The object.
     * @param c
     *            The context (optional).
     */
    public void add(Resource s, URI p, Value o, Resource c);

    /**
     * Add a statement to the buffer.
     * <p>
     * Note: The context parameter (<i>c</i>) is NOT used. The database at
     * this time is either a triple store or a triple store with statement
     * identifiers, and in neither case is the context used.
     * 
     * @param s
     *            The subject.
     * @param p
     *            The predicate.
     * @param o
     *            The object.
     * @param c
     *            The context (optional).
     * @param type
     *            The statement type (optional).
     */
    public void add(Resource s, URI p, Value o, Resource c, StatementEnum type);

    /**
     * Set the canonicalizing map for blank nodes based on their ID. This allows
     * you to reuse the same map across multiple {@link IStatementBuffer}
     * instances. For example, the {@link BigdataSail} does this so that the
     * same bnode map is used throughout the life of a {@link SailConnection}.
     * While RIO provides blank node correlation within a given source, it does
     * NOT provide blank node correlation across sources. You need to use this
     * method to do that.
     * <p>
     * Note: It is reasonable to expect that the bnodes map is used by
     * concurrent threads. For this reason, the map SHOULD be thread-safe. This
     * can be accomplished either using {@link Collections#synchronizedMap(Map)}
     * or a {@link ConcurrentHashMap}. However, implementations MUST still be
     * synchronized on the map reference across operations which conditionally
     * insert into the map in order to make that update atomic and thread-safe.
     * Otherwise a race condition exists for the conditional insert and
     * different threads could get incoherent answers.
     * 
     * @param bnodes
     *            The blank nodes map.
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>.
     * @throws IllegalStateException
     *             if the map has already been allocated.
     */
    public void setBNodeMap(Map<String, BigdataBNode> bnodes);
    
}
