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
 * Created on Aug 15, 2012
 */
package com.bigdata.bop.controller;

import java.io.Serializable;
import java.util.UUID;

import com.bigdata.bop.IQueryAttributes;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NamedSolutionSetRefUtility;
import com.bigdata.bop.engine.IRunningQuery;

/**
 * An interface specifying the information required to locate a named solution
 * set.
 * <p>
 * Note: There are two basic ways to locate named solution sets. Either they are
 * attached to the {@link IQueryAttributes} of an {@link IRunningQuery} (query
 * local) -or- they are located using the <em>namespace</em> and
 * <em>timestamp</em> of an MVCC view (this works for both cached and durable
 * named solution sets). Either {@link #getQueryId()} will be non-
 * <code>null</code> or {@link #getNamespace()} will be non-<code>null</code> ,
 * but not both.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface INamedSolutionSetRef extends Serializable{

    /**
     * The {@link UUID} of the {@link IRunningQuery} which generated the named
     * solution set. When non-<code>null</code>, this is where you need to look
     * to find the data.
     */
    UUID getQueryId();

    /**
     * The namespace associated with the KB view -or- <code>null</code> if the
     * named solution set is attached to an {@link IRunningQuery}.
     * 
     * @see #getQueryId()
     * @see #getTimestamp();
     */
    String getNamespace();

    /**
     * The timestamp associated with the KB view.
     * <p>
     * Note: This MUST be ignored if {@link #getNamespace()} returns
     * <code>null</code>.
     */
    long getTimestamp();
    
    /**
     * The application level name for the named solution set (as used in a
     * SPARQL query or update operation).
     */
    String getLocalName();

    /**
     * Return the fully qualified name of the named solution set. This is the
     * name that is used to resolve the named solution set against a durable
     * store or cache.
     * <p>
     * Note: When the named solution set is attached to an
     * {@link IQueryAttributes}, the {@link INamedSolutionSetRef} itself is used
     * as the key into the {@link IQueryAttributes}. In this case, the
     * combination of the {@link #getQueryId()} (identifying the query) and the
     * {@link INamedSolutionSetRef} object together provide the equivalent of a
     * fully qualified name.
     */
    String getFQN();
    
    /**
     * The ordered set of variables that specifies the ordered set of components
     * in the key for the desired index over the named solution set (required,
     * but may be an empty array).
     */
    IVariable[] getJoinVars();

    /**
     * Return a human readable representation which can be decoded by
     * {@link NamedSolutionSetRefUtility#valueOf(String)}.
     */
    String toString();

}
