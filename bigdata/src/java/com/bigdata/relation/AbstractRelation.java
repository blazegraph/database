/*

 Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jun 30, 2008
 */

package com.bigdata.relation;

import java.util.Properties;

import com.bigdata.btree.IIndex;
import com.bigdata.journal.IIndexManager;
import com.bigdata.relation.accesspath.IKeyOrder;

/**
 * Base class for {@link IRelation} and {@link IMutableRelation} impls.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 *            The generic type of the [E]lements of the relation.
 * 
 * @todo It would be interesting to do a GOM relation with its secondary index
 *       support and the addition of clustered indices. We would then get
 *       efficient JOINs via the rules layer for free and a high-level query
 *       language could be mapped onto those JOINs.
 */
abstract public class AbstractRelation<E> extends AbstractResource<IRelation<E>> implements
        IMutableRelation<E> {

    /**
     * 
     */
    protected AbstractRelation(IIndexManager indexManager, String namespace,
            Long timestamp, Properties properties) {

        super(indexManager, namespace, timestamp, properties);

    }

    /**
     * The fully qualified name of the index.
     * 
     * @param keyOrder
     *            The natural index order.
     * 
     * @return The index name.
     */
    abstract public String getFQN(IKeyOrder<? extends E> keyOrder);
    
    /**
     * The index.
     * 
     * @param keyOrder
     *            The natural index order.
     *            
     * @return The index.
     */
    public IIndex getIndex(IKeyOrder<? extends E> keyOrder) {

        return getIndexManager().getIndex(getFQN(keyOrder), getTimestamp());
        
    }

}
