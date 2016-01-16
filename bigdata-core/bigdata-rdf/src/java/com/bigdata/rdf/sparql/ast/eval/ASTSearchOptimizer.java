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
import com.bigdata.rdf.store.BDS;

/**
 * Translate {@link BDS#SEARCH} and related magic predicates into a
 * {@link ServiceNode} which will invoke the bigdata search engine.
 * 
 * <pre>
 * with {
 *    select ?subj ?score
 *    where {
 *      ?lit bds:search "foo" .
 *      ?lit bds:relevance ?score .
 *      ?subj ?p ?lit .
 *    }
 *   ORDER BY DESC(?score)
 *   LIMIT 10
 *   OFFSET 0 
 * } as %searchSet1
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class ASTSearchOptimizer extends ASTSearchOptimizerBase {

    static final Set<URI> searchUris;
   
    static {
      
        final Set<URI> set = new LinkedHashSet<URI>();
  
        set.add(BDS.SEARCH);
        set.add(BDS.RELEVANCE);
        set.add(BDS.RANK);
        set.add(BDS.MAX_RANK);
        set.add(BDS.MIN_RANK);
        set.add(BDS.MAX_RELEVANCE);
        set.add(BDS.MIN_RELEVANCE);
        set.add(BDS.MATCH_ALL_TERMS);
        set.add(BDS.MATCH_EXACT);
        set.add(BDS.SUBJECT_SEARCH);
        set.add(BDS.SEARCH_TIMEOUT);
        set.add(BDS.MATCH_REGEX);
        set.add(BDS.RANGE_COUNT);
      
        searchUris = Collections.unmodifiableSet(set);
  
    }
   
    @Override
    protected Set<URI> getSearchUris() {
       return searchUris;
    }

    @Override
    protected String getNamespace() {
       return BDS.NAMESPACE;
    }

    @Override
    protected URI getSearchPredicate() {
       return BDS.SEARCH;
    }
    
}
