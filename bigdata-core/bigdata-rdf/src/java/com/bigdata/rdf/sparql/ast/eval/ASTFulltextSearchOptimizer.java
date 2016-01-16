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
 * Created on Sep 8, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.openrdf.model.URI;

import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.service.fts.FTS;

/**
 * Translate {@link FTS#SEARCH} and related magic predicates into a
 * {@link ServiceNode} which will invoke the bigdata search engine.
 * 
 * <pre>
 * with {
 *    select ?subj ?score
 *    where {
 *      ?res fts:search "foo" .
 *      ?res fts:endpoint "http://my.solr.endpoint"
 *      ?res fts:relevance ?score .
 *    }
 * } as %searchSet1
 * </pre>
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class ASTFulltextSearchOptimizer extends ASTSearchOptimizerBase {

    static public final Set<URI> searchUris;
   
    static {
      
        final Set<URI> set = new LinkedHashSet<URI>();

        set.add(FTS.SEARCH);
        set.add(FTS.ENDPOINT);
        set.add(FTS.ENDPOINT_TYPE);
        set.add(FTS.PARAMS);
        set.add(FTS.SEARCH_RESULT_TYPE);
        set.add(FTS.TIMEOUT);
        set.add(FTS.SCORE);
        set.add(FTS.SNIPPET);
        set.add(FTS.SEARCH_FIELD);
        set.add(FTS.SNIPPET_FIELD);
        set.add(FTS.SCORE_FIELD);
        
        searchUris = Collections.unmodifiableSet(set);
    }
   
    @Override
    protected Set<URI> getSearchUris() {
       return searchUris;
    }

    @Override
    protected String getNamespace() {
       return FTS.NAMESPACE;
    }

    @Override
    protected URI getSearchPredicate() {
       return FTS.SEARCH;
    }
}
