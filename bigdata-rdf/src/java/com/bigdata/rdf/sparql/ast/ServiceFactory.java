/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Sep 8, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Map;

import org.openrdf.model.URI;

import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Factory for creating objects which can talk to registered service URIs within
 * the same JVM.
 */
public interface ServiceFactory {

    /**
     * Create a service invocation object.
     * 
     * @param store
     *            The {@link AbstractTripleStore}.
     * @param groupNode
     *            The graph pattern parameter to the service.
     * @param serviceURI
     *            The Service URI.
     * @param exprImage
     *            The text "image" of the original SPARQL SERVICE clause
     *            (optional). This "image" contains the original group graph
     *            pattern, which is what gets sent to a remote SPARQL end point
     *            when we evaluate the SERVICE node. Because the original
     *            "image" of the graph pattern is being used, we also need to
     *            have the prefix declarations so we can generate a valid SPARQL
     *            request.
     * @param prefixDecls
     *            The prefix declarations for the SPARQL query from which the
     *            <i>exprImage</i> was taken (optional). This is needed IFF we
     *            need to generate a SPARQL query for a remote SPARQL end point
     *            when we evaluate the SERVICE request.
     * 
     * @return The object which can be used to evaluate the service on the graph
     *         pattern.
     */
    ServiceCall<? extends Object> create(
            final AbstractTripleStore store,
            final IGroupNode<IGroupMemberNode> groupNode,
            final URI serviceURI,
            final String exprImage,
            final Map<String,String> prefixDecls);

}