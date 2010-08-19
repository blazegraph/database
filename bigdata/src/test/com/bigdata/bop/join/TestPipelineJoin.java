/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Aug 18, 2010
 */

package com.bigdata.bop.join;

import junit.framework.TestCase2;

import com.bigdata.bop.ChunkedOrderedIteratorOp;
import com.bigdata.bop.EmptyBindingSet;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IPredicate;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;

/**
 * Unit tests for the {@link PipelineJoin} operator.
 * <p>
 * Note: The operators to map binding sets over shards are tested independently.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Write unit tests for simple joins. This can be done without using an
 *       {@link IPredicate} since the right operand is a
 *       {@link ChunkedOrderedIteratorOp}.
 * 
 * @todo Write unit tests for joins where the right operand is optional.
 * 
 * @todo Write unit tests for correct reporting of join statistics.
 * 
 * @todo Write unit tests for star-joins (in their own test suite).
 */
public class TestPipelineJoin extends TestCase2 {

    /**
     * 
     */
    public TestPipelineJoin() {
    }

    /**
     * @param name
     */
    public TestPipelineJoin(String name) {
        super(name);
    }

    /**
     * Return an {@link IAsynchronousIterator} that will read a single,
     * empty {@link IBindingSet}.
     * 
     * @param bindingSet
     *            the binding set.
     */
    protected ThickAsynchronousIterator<IBindingSet[]> newBindingSetIterator(
            final IBindingSet bindingSet) {

        return new ThickAsynchronousIterator<IBindingSet[]>(
                new IBindingSet[][] { new IBindingSet[] { bindingSet } });

    }

    /**
     * Explore how we could setup a unit test without using an access path for
     * this query, or better yet, for a single join operator from this query.
     * That would probably have to happen at the ChunkTask level since the
     * AccessPathTask is going to apply an {@link IBindingSet} to the
     * {@link IPredicate} to read on an {@link IAccessPath}.
     * 
     * <pre>
     * :- ..., POS(A loves B), SPO(B loves C).
     * 
     *      and the following intermediate results from the POS shard:
     * 
     *      B0:[A=John, B=Mary, ...]
     *      B1:[A=Mary, B=Paul, ...]
     *      B2:[A=Paul, B=Leon, ...]
     *      B3:[A=Leon, B=Paul, ...]
     * 
     *      and the following tuples read from the SPO shard:
     * 
     *      T0:(John loves Mary)
     *      T1:(Mary loves Paul)
     *      T2:(Paul loves Leon)
     *      T3:(Leon loves Paul)
     * 
     *      then we have the following joins:
     * 
     *      (T2, B3) // T2:(Paul loves Leon) with B3:[A=Leon, B=Paul, ...].
     *      (T3, B2) // T3:(Leon loves Leon) with T2:[A=Paul, B=Leon, ...].
     * </pre>
     */
    public void test_pipelineJoin() {

        // source for the 1st join dimension.
        final IAsynchronousIterator<IBindingSet[]> source = newBindingSetIterator(EmptyBindingSet.INSTANCE);

        fail("write tests");

    }

}
