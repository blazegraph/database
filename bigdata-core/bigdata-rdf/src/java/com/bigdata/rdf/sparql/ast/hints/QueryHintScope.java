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
 * Created on Oct 26, 2011
 */

package com.bigdata.rdf.sparql.ast.hints;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.optimizers.ASTQueryHintOptimizer;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;

/**
 * Type safe enumeration for the scope of a query hint. The {@link URI} for each
 * scope is {@value QueryHints#NAMESPACE} plus the name of the enumeration
 * value. For example, <code>http://www.bigdata.com/queryHints#Group</code>
 * would apply to the entire group in which that query hint was found.
 * 
 * @see QueryHints
 * @see ASTQueryHintOptimizer
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public enum QueryHintScope {

    /**
     * The entire query.
     */
    Query(new URIImpl(QueryHints.NAMESPACE + "Query")),
    /**
     * The query or subquery in which the query hint appears (any of the
     * {@link QueryBase} instances).
     * 
     * @see QueryRoot
     * @see SubqueryRoot
     * @see NamedSubqueryRoot
     */
    SubQuery(new URIImpl(QueryHints.NAMESPACE + "SubQuery")),
    /**
     * The group in which the query hint appears and any direct non-group
     * children within that group.
     */
    Group(new URIImpl(QueryHints.NAMESPACE + "Group")),
    /**
     * The group in which the query hint appears and any children of that group.
     * This does not apply to things within {@link ServiceNode}s or
     * {@link SubqueryRoot}s.
     */
    GroupAndSubGroups(new URIImpl(QueryHints.NAMESPACE + "GroupAndSubGroups")),
    /**
     * The query hint binds on the previous non-query hint AST node which is not
     * itself a query hint. This may be used to bind a query hint on a
     * {@link StatementPatternNode}, a {@link JoinGroupNode}, a
     * {@link UnionNode}, a {@link ServiceNode}, etc. This DOES NOT bind the
     * query hint on the children of that AST node.
     */
    Prior(new URIImpl(QueryHints.NAMESPACE + "Prior"));

    private QueryHintScope(final URI uri) {
        this.uri = uri;
    }

    private final URI uri;

    public URI getURI() {
        return uri;
    }
    
    public static QueryHintScope valueOf(final URI uri) {
        if(uri == null)
            throw new IllegalArgumentException();
        if (!QueryHints.NAMESPACE.equals(uri.getNamespace())) {
            throw new IllegalArgumentException("Wrong namespace: expected="
                    + QueryHints.NAMESPACE + ", actual="
                    + uri.getNamespace());
        }
        final String localName = uri.getLocalName();
        if (Query.name().equals(localName)) {
            return Query;
        }
        if (SubQuery.name().equals(localName)) {
            return SubQuery;
        }
        if (Group.name().equals(localName)) {
            return Group;
        }
        if (GroupAndSubGroups.name().equals(localName)) {
            return GroupAndSubGroups;
        }
//        if (BGP.name().equals(localName)) {
//            return BGP;
//        }
        if (Prior.name().equals(localName)) {
            return Prior;
        }
        throw new IllegalArgumentException("Unknown scope: " + localName);
    }

}
