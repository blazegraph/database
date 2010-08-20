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
import java.util.UUID;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.UnisolatedReadWriteIndex;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TemporaryRawStore;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.striterator.IKeyOrder;

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
    protected AbstractRelation(final IIndexManager indexManager,
            final String namespace, final Long timestamp,
            final Properties properties) {

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
    public String getFQN(final IKeyOrder<? extends E> keyOrder) {
        
        return getNamespace() + "." + keyOrder.getIndexName();
        
    }

    /**
     * Return the index for the {@link IKeyOrder} the timestamp for this view of
     * the relation.
     * 
     * @param keyOrder
     *            The natural index order.
     * 
     * @return The index -or- <code>null</code> iff the index does not exist as
     *         of the timestamp for this view of the relation.
     * 
     * @see #getIndex(String)
     * 
     * @todo For efficiency the concrete implementations need to override this
     *       saving a hard reference to the index and then use a switch like
     *       construct to return the correct hard reference. This behavior
     *       should be encapsulated.
     */
    public IIndex getIndex(final IKeyOrder<? extends E> keyOrder) {

        return getIndex(getFQN(keyOrder));
        
    }

    /**
     * Return the named index using the timestamp for this view of the relation
     * (core impl).
     * <p>
     * While both the {@link IBigdataFederation} imposes the
     * {@link ConcurrencyManager} on all access to a named index, neither the
     * {@link Journal} nor the {@link TemporaryRawStore} does this. Therefore
     * this method encapsulates the unisolated index for the latter classes in
     * order to impose the correct concurrency constraints. It does this using
     * an {@link UnisolatedReadWriteIndex}. This allows the caller to use the
     * returned index view without regard to concurrency controls (it will
     * appear to be a thread-safe object).
     * 
     * @param fqn
     *            The fully qualified name of the index.
     * 
     * @return The named index -or- <code>null</code> iff the named index does
     *         not exist as of that timestamp.
     * 
     * @throws IllegalArgumentException
     *             if <i>name</i> is <code>null</code>.
     * 
     * @todo hard references to the indices must be dropped when an abort is
     *       processed. this is a bit awkward. the abort() could be raised into
     *       the relation container (it is for the AbstractTripleStore) and into
     *       the relation itself to facilitate this. alternatively the index
     *       objects themselves could be notified of an abort and make
     *       themselves invalid (this really only applies to the unisolated
     *       index and to indices isolated by a transaction).
     */
    public IIndex getIndex(final String fqn) {

        if (fqn == null)
            throw new IllegalArgumentException();

        final IIndexManager indexManager = getIndexManager();

        final long timestamp = getTimestamp();

        IIndex ndx = indexManager.getIndex(fqn, timestamp);

        if (ndx != null
                && timestamp == ITx.UNISOLATED
                && (indexManager instanceof Journal || indexManager instanceof TemporaryStore)) {

            if(log.isDebugEnabled()) {
                
                log.debug("Imposing read-write concurrency controls on index: name="
                                + fqn);
                
            }
            
            ndx = new UnisolatedReadWriteIndex(ndx);

        }

        return ndx;

    }

    /**
     * Factory for {@link IndexMetadata}.
     * 
     * @param name
     *            The fully qualified index name.
     * 
     * @return A new {@link IndexMetadata} object for that index.
     */
    protected IndexMetadata newIndexMetadata(final String name) {

        final IndexMetadata metadata = new IndexMetadata(getIndexManager(),
                getProperties(), name, UUID.randomUUID());

        return metadata;

    }

}
