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
 * Created on Apr 9, 2012
 */
package com.bigdata.bop.solutions;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.controller.INamedSolutionSetRef;
import com.bigdata.btree.BTree;
import com.bigdata.btree.ICheckpointProtocol;
import com.bigdata.htree.HTree;
import com.bigdata.rdf.sparql.ast.ISolutionSetStats;
import com.bigdata.relation.accesspath.AccessPath;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.stream.Stream;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * Interface for durable solution sets.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 *         TODO Provide {@link ISolutionSet} implementations derived from
 *         {@link HTree} and {@link BTree}.
 *         <P>
 *         I am not sure that we will require this. We can probably use the
 *         {@link SolutionSetStream} for named solution sets for quite a while
 *         without supporting declaration of named solution sets with specific
 *         join variables. However, if we do add support for this, then we need
 *         to reconcile the {@link HTree} version with the hash join code and
 *         {@link INamedSolutionSetRef} and also add an {@link IAccessPath} for
 *         {@link HTree} and {@link Stream} backed solution sets so they can be
 *         played out through a simple iterator model. The {@link AccessPath} is
 *         relation specific, but it could be relayered to provide support for
 *         the {@link BTree} backed solution sets.
 */
public interface ISolutionSet extends ICheckpointProtocol {

    /**
     * Return the {@link ISolutionSetStats} for the saved solution set. This may
     * be used for query planning and should not require the backing solutions
     * to be materialized.
     * 
     * @return The {@link ISolutionSetStats}.
     */
    public ISolutionSetStats getStats();
    
//    /**
//     * Return the CREATE schema for the solution set (this is the metadata used
//     * to provision the characteristics of the solution set).
//     * 
//     * TODO This should be stored in a "root" on the checkpoint record.
//     */
//    public ISPO[] getDeclation();
    
    /**
     * Visit all entries in the index in the natural order of the index.
     */
    public ICloseableIterator<IBindingSet> scan();

    /**
     * Return an {@link ICloseableIterator} reading the solutions from the
     * stream.
     */
    public ICloseableIterator<IBindingSet[]> get();
    
    /**
     * Replace the contents of the stream with the solutions read from the
     * source.
     * 
     * @param src
     *            The source.
     */
    public void put(final ICloseableIterator<IBindingSet[]> src);

//    /**
//     * The {@link IVariable[]} specifying the join variables (required). The
//     * order of the entries is used when forming the as-bound keys for the hash
//     * table. Duplicate elements and null elements are not permitted.
//     * 
//     * TODO We will also need a method to report the JOIN_VARS if the backing
//     * data structure is an HTree or BTree. [Really, what we need is a method to
//     * describe how the index key will be generated, or just a method to
//     * generate the key. Also, it is possible that
//     * {@link ISolutionSetStats#getAlwaysBound} will report variables which are
//     * always bound but which are not part of the key for the index.
//     */
//    public String[] getJoinVars();

}
