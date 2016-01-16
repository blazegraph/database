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

package com.bigdata.gom.gpo;

import java.util.Map;
import java.util.Set;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.gom.om.IObjectManager;
import com.bigdata.gom.skin.GenericSkinRegistry;
import com.bigdata.rdf.model.BigdataResource;

/**
 * A generic object has identity and an extensible, loosely-typed property set.
 * When the value of a property is another generic object, the property models a
 * directed many-to-one association. Many-to-many linking patterns are modeled
 * using intermediate objects and a convenience API. Reverse navigation for
 * associations is automatically aggregated by the association target within a
 * {@link ILinkSet link set}. Associations and link sets provide an uniform,
 * efficient and scalable API for object navigation. Link sets may be
 * {@link ILinkSetIndex indexed }for efficient search.
 * <p>
 * 
 * Extensible behavior is realized by wrapping up a generic object within some
 * application specific interface. Generic object implementations should be
 * considered as <code>final</code>. Specific backends may support
 * subclassing but use of subclassing will lock an application into a specific
 * GOM platform. Instead applications layer additional behavior onto generic
 * objects using "skins" - see {@link #asClass( Class )}. A skin delegates the
 * management of persistent state to a backing generic object. The use of skins
 * provides transparent migration of application code across different GOM
 * implementations.
 * <p>
 * 
 * The generic object model provides transparent and scalable persistence when
 * using an appropriate backing store. The backing store is selected and
 * configured using the {@link ObjectManagerFactory}.
 * <p>
 */
public interface IGPO extends IGenericSkin // @todo , INativeTransaction?
{

    /*
     * TODO Need something about the materialized facets. This would need to be
     * reconciled with the RDFS+ Schema.
     */

    /**
     * Returns the persistent identity of the generic object.
     */
    BigdataResource getId();

    /**
     * Returns true iff <i>other</i> has the same identity as <i>this</i>
     * generic object and is in the same {@link IObjectManager} scope (this is
     * just <code>this == other</code> since the {@link IObjectManager}
     * guarantees that {@link IGPO}s are canonical for a given {@link Resource}
     * within the scope of the {@link IObjectManager}).
     */
    boolean equals(Object other);

    /**
     * Return the hash code of the identity of the generic object. (The hash
     * code is NOT based on the internal state of the generic object.)
     */
    int hashCode();

    /**
     * Return the {@link IObjectManager} used to materialize or create this
     * generic object.
     */
    IObjectManager getObjectManager();

    // ************************************************************
    // ************************* get / set ************************
    // ************************************************************

    /*
     * All methods return mutable collections. Neither the GPO's statement set
     * nor the reverse link set are pre-materialized.
     */

    /** Return first Value for property or null if no Value for that property. */
    Value getValue(URI property);

    /** Replace (self,p,?x) with (self,p,newValue). */
    void setValue(URI property, Value newValue);

    /** Assert (self,p,newValue). */
    void addValue(URI property, Value newValue);

//    /** Assert (self,p,target). */
//    void addLink(URI property,IGPO target);

    /** Return the {@link IGPO} modeling the link iff it exists. */
    IGPO getLink(URI property, IGPO target);

    /** Remove (self,p,oldValue) if found. */
    void removeValue(URI property, Value oldValue);
    
    /** Remove all (self,p,?). */
    void removeValues(URI property);

    /** All (self,?p,?o). */
    Set<Statement> getStatements();

    /** All ?y where (self,p,?y). */
    Set<Value> getValues(URI property);

    /** All ?y where (?y,?p,self). */
    Set<IGPO> getLinksIn();

    /** All ?y where (?y,p,self). */
    ILinkSet getLinksIn(URI property);

    /** All ?y where (self,?p,?y) and ?y is Resource. */
    Set<IGPO> getLinksOut();

    /** All ?y where (self,p,?y) and ?y is Resource. */
    ILinkSet getLinksOut(URI property);

    /** Exists (?x,linkSet.property,self). */
    boolean isMemberOf(ILinkSet linkSet);

    /** Exists (self,p,?y). */
    boolean isBound(URI property);

    /**
     * Return a map giving the range count for each reverse link property.
     * <pre>
     * SELECT ?p, COUNT(*) WHERE { ?o ?p <s> } GROUP BY ?p
     * </pre>
     */
    Map<URI,Long> getReverseLinkProperties();
    
    /**
     * Removes the persistent object. Any links to this generic object are also
     * removed. Any link sets collected by this generic object are removed.
     * <p>
     * 
     * While this clears all link sets held by the generic object, the members
     * of those link sets are NOT explicitly removed since they may participate
     * in other associations.
     * <p>
     * 
     * The {@link IGPO} interface may no longer be used for this object if this
     * operation is successful.
     * <p>
     */
    void remove();

    /**
     * Returns an transient object that wraps the same persistent object but
     * exposes the behavior identified by the identified interface. The
     * interface effectively names the behavior that will be wrapped around the
     * persistent data. If the current object already implements the desired
     * interface, then it is simply returned. If a suitable {@link IGenericSkin}
     * has already been minted for this generic object, then it is simply
     * returned. Otherwise an implementation class for the interface MUST have
     * been registered with the runtime and MUST implement a constructor
     * accepting a single {@link IGenericSkin} argument.
     * <p>
     * 
     * The life cycle of a skin object returned by this method MUST be no less
     * than the in-memory life cycle of the generic object for which the skin
     * was minted. This constraint makes it possible to have transient yet
     * expensive initialization, such as caches, on skin objects.
     * <p>
     * 
     * @exception UnsupportedOperationException
     *                If the runtime is unable to identify an implementation
     *                class for the specified interface.
     * 
     * @return An object that wraps the same persistent data but implements the
     *         desired interface.
     * 
     * @see IGenericSkin
     * @see GenericSkinRegistry
     */
    IGenericSkin asClass(Class theClassOrInterface);

    /**
     * Force the full materialization of the {@link IGPO}.
     * 
     * @return This {@link IGPO}.
     */
    IGPO materialize();
    
    /**
     * @return a pretty printed representation of the GPO
     */
	String pp();
	
	/**
	 * FIXME: this method will be moved to an as yet unnamed derived class
	 * that will become the superclass for alchemist generated subclasses.
	 */
	IGPO getType();

}
