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

package com.bigdata.bop.ap;

import java.util.Map;

import com.bigdata.bop.AbstractChunkedOrderedIteratorOp;
import com.bigdata.bop.BOp;
import com.bigdata.bop.ChunkedOrderedIteratorOp;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.fed.MapBindingSetsOverShards;
import com.bigdata.rdf.rules.TMUtility;
import com.bigdata.relation.RelationFusedView;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.proxy.IRemoteChunkedIterator;
import com.bigdata.striterator.ChunkedOrderedStriterator;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.ibm.icu.impl.ByteBuffer;

/**
 * An operator which returns the union of two {@link IPredicate}s. Elements are
 * consumed first from the left predicate and then from the right predicate.
 * This operator does not cross network boundaries. An intermediate send /
 * receive operator pattern must be applied when this operator is used in a
 * scale-out context.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo I have some basic questions about the ability to use a UNION of two
 *       predicates in scale-out. I think that this might be more accurately
 *       modeled as the UNION of two joins. That is, rather than:
 * 
 *       <pre>
 *       JOIN( ...,
 *             UNION( foo.spo(A,loves,B),
 *                    bar.spo(A,loves,B) )
 *             )
 * </pre>
 *       using
 *       <pre>
 *       UNION( JOIN( ..., foo.spo(A,loves,B) ),
 *              JOIN( ..., bar.spo(A,loves,B) )
 *              )
 * </pre>
 *       which would be a binding set union rather than an element union.
 * 
 * @todo This was historically handled by {@link RelationFusedView} which should
 *       be removed when this class is implemented.
 * 
 * @todo The {@link TMUtility} will have to be updated to use this operator
 *       rather than specifying multiple source "names" for the relation of the
 *       predicate.
 * 
 * @todo The FastClosureRuleTask will also need to be updated to use a
 *       {@link Union} rather than a {@link RelationFusedView}.
 * 
 * @todo It would be a trivial generalization to make this an N-ary union.
 * 
 * @todo A similar operator could be defined where child operands to execute
 *       concurrently and the result is no longer strongly ordered.
 * 
 * @todo Implement the send/receive pattern.
 *       <p>
 *       This COULD be done using {@link IRemoteChunkedIterator} if the send and
 *       receive operators are appropriately decorated in order to pass the
 *       proxy object along.
 *       <p>
 *       This SHOULD be implemented using an NIO direct {@link ByteBuffer}
 *       pattern similar to {@link MapBindingSetsOverShards}.
 */
public class Union<E> extends AbstractChunkedOrderedIteratorOp<E> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * @param left
     * @param rigtht
     * @param annotations
     */
    public Union(final ChunkedOrderedIteratorOp<E> left,
            final ChunkedOrderedIteratorOp<E> right,
            final Map<String, Object> annotations) {

        super(new BOp[] { left, right }, annotations);

    }

    @SuppressWarnings("unchecked")
    protected ChunkedOrderedIteratorOp<E> left() {
        return (ChunkedOrderedIteratorOp<E>)args[0];
    }
    
    @SuppressWarnings("unchecked")
    protected ChunkedOrderedIteratorOp<E> right() {
        return (ChunkedOrderedIteratorOp<E>)args[1];
    }

    @SuppressWarnings("unchecked")
    public IChunkedOrderedIterator<E> eval(final IBigdataFederation<?> fed,
            final IJoinNexus joinNexus) {

        return (IChunkedOrderedIterator<E>) new ChunkedOrderedStriterator<IChunkedOrderedIterator<E>, E>(//
                left().eval(fed, joinNexus)).append(//
                right().eval(fed, joinNexus)//
                );

    }

}
