/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * Created on Apr 12, 2012
 */

package com.bigdata.rdf.sparql.ast.eval;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.sparql.ast.ISolutionSetStats;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.cache.IDescribeCache;
import com.bigdata.rdf.sparql.ast.optimizers.IASTOptimizer;
import com.bigdata.rdf.sparql.ast.ssets.ISolutionSetManager;
import com.bigdata.rdf.spo.SPORelation;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Interface providing access to various things of interest when preparing and
 * evaluating a query or update operation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IEvaluationContext {

    /**
     * Some summary statistics about the exogenous solution sets. These are
     * computed by {@link AST2BOpUtility#convert(AST2BOpContext, IBindingSet[])}
     * before it begins to run the {@link IASTOptimizer}s.
     */
    ISolutionSetStats getSolutionSetStats();

    /**
     * The timestamp or transaction identifier associated with the view.
     */
    long getTimestamp();

    /**
     * Return <code>true</code> if we are running on a cluster.
     */
    boolean isCluster();

    /**
     * Return <code>true</code> iff the target {@link AbstractTripleStore} is in
     * quads mode.
     */
    boolean isQuads();

    /**
     * Return <code>true</code> iff the target {@link AbstractTripleStore} is in
     * SIDS mode.
     */
    boolean isSIDs();

    /**
     * Return <code>true</code> iff the target {@link AbstractTripleStore} is in
     * triples mode.
     */
    boolean isTriples();

    /**
     * Return the namespace of the {@link AbstractTripleStore}.
     */
    String getNamespace();

    /**
     * Return the namespace of the {@link SPORelation}.
     */
    String getSPONamespace();

    /**
     * Return the namespace of the {@link LexiconRelation}.
     */
    String getLexiconNamespace();

    /**
     * Return the timestamp which will be used to read on the lexicon.
     * <p>
     * Note: This uses the timestamp of the triple store view unless this is a
     * read/write transaction, in which case we need to use the last commit
     * point in order to see any writes which it may have performed (lexicon
     * writes are always unisolated).
     */
    long getLexiconReadTimestamp();
    
    /**
     * Return the database.
     */
    AbstractTripleStore getAbstractTripleStore();
    
    /**
	 * Return the manager for named solution sets (experimental feature).
	 * 
	 * @return The manager -or- <code>null</code>.
	 */
    ISolutionSetManager getSolutionSetManager();

    /**
     * Return the cache for described resources (experimental feature).
     * 
     * @return The cache -or- <code>null</code>.
     * 
     * @see QueryHints#DESCRIBE_CACHE
     */
    IDescribeCache getDescribeCache();

    /**
     * Resolve the pre-existing named solution set returning its
     * {@link ISolutionSetStats}.
     * 
     * @param localName
     *            The local name of the named solution set.
     * 
     * @return The {@link ISolutionSetStats}
     * 
     * @throws RuntimeException
     *             if the named solution set can not be found.
     */
    ISolutionSetStats getSolutionSetStats(String name);
 
//    /**
//     * Resolve a pre-existing named solution set.
//     * 
//     * @param localName
//     *            The local name of the named solution set.
//     *            
//     * @return The {@link ISolutionSet}
//     * 
//     * @throws RuntimeException
//     *             if the named solution set can not be found.
//     */
//    ICloseableIterator<IBindingSet[]> getSolutionSet(String name);
    
}
