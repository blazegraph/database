/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Mar 19, 2012
 */

package com.bigdata.gom.om;

import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;

import com.bigdata.gom.gpo.IGPO;
import com.bigdata.rdf.model.BigdataValueFactory;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * The object manager is the abstraction for a connection the back end.
 */
public interface IObjectManager extends INativeTransaction {

    /**
     * @return the UUID that identifies this ObjectManager
     * 
     * @deprecated Why do we need this? It should be hidden in how we generate
     *             URIs, not part of the public API.
     */
    UUID getID();

    /**
     * Return a canonical {@link IGPO} for the {@link Resource} (canonical
     * within the scope of this object manager) and never <code>null</code>.
     * 
     * @param id
     *            The {@link Resource}.
     * 
     * @return The {@link IGPO}.
     * 
     * @see IGPO#getId()
     */
    /*
     * TODO We need to work through lazy materialization, materialization of
     * facets corresponding to class model information, and reconcilation of
     * property values for an object on commit with respect to the class model
     * information.
     * 
     * TODO How can we deal with blank nodes in this context?
     */
    // Return canonical IGPO for Resource (including Statement) w/in OM scope and never null.
    IGPO getGPO(Resource id);

    IGPO getGPO(Statement stmt);

    /**
     * Partially materialize {@link IGPO}s from a stream of {@link Statement}s.
     * 
     * @param itr
     *            The {@link Statement}s.
     *            
     * @return A hard reference collection that will keep the any materialized
     *         {@link IGPO}s from being finalized before the caller has a chance
     *         to do something with them.
     */
    Map<Resource,IGPO> initGPOs(final ICloseableIterator<Statement> itr);

    /**
     * An iterator that visits the weak reference values in the running object
     * table. You must test each weak reference in order to determine whether
     * its value has been cleared as of the moment that you request that value.
     * The entries visited by the iterator are not "touched" so the use of the
     * iterator will not cause them to be retained any longer than they
     * otherwise would have been retained.
     * 
     * TODO We could do one-step look ahead and buffer a hard reference to the
     * next {@link IGPO} to be visited. That would allow us to eliminate the
     * {@link WeakReference} from this method signature.
     */
    Iterator<WeakReference<IGPO>> getGPOs();
    
    /**
     * Ensure Statements are materialized for gpo's Resource:
     * <code>DESCRIBE ?s</code>.
     */
    void materialize(IGPO gpo);

    /**
     * Close the object manager, which terminates its connection with the
     * backing store. Any open concurrent or nested/native transactions are
     * flushed to the store. The object manager can not be used after it has
     * been closed.
     * 
     * @exception IllegalStateException
     *                if the store is already closed.
     */
    void close();

    /**
     * Return <code>true</code> iff the object manager is backed by some
     * persistence layer.
     */
    boolean isPersistent();

    /**
     * The value factory for the KB instance associated with this object manager
     * view.
     */
    BigdataValueFactory getValueFactory();
    
    /**
     * 
     */
    ICloseableIterator<BindingSet> evaluate(String queryStr);

    /**
     * 
     */
    void execute(String updateStr);

    ICloseableIterator<Statement> evaluateGraph(String query);

	/**
	 * The ObjectManager is able to assign automatic ids for a new object.  These
	 * will be of the form "gpo:#[genid]"
	 * 
	 * @return a new GPO
	 */
	IGPO createGPO();

	/**
	 * Simple save/recall interface that the ObjectManager provides to simplify
	 * other pattern implementations.  Internally it uses a NameManager GPO
	 */
    @Deprecated // Just use the URI directly...
	void save(URI key, Value value);

	/**
	 * Simple save/recall interface that the ObjectManager provides to simplify
	 * other pattern implementations.  Internally it uses a NameManager GPO
	 */
    @Deprecated // Just use the URI directly...
	Value recall(URI key);
	
	IGPO recallAsGPO(URI key);
	
	/**
	 * Return the list of names that have been used to save references. These
	 * are the properties of the internal NameManager.
	 */
    @Deprecated // Just use URIs directly...
	Iterator<URI> getNames();

	/**
	 * Remove all assertions involving the specified object.
	 */
	void remove(IGPO gpo);

}
