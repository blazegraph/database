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
 * Created on Aug 19, 2010
 */

package com.bigdata.bop.ap;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.journal.IIndexManager;
import com.bigdata.relation.AbstractRelation;
import com.bigdata.relation.locator.ILocatableResource;
import com.bigdata.striterator.AbstractKeyOrder;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;

/**
 * Test relation composed of {@link E} elements with a single primary index.
 * <p>
 * Note: This has to be public in order to be an {@link ILocatableResource}.
 */
public class R extends AbstractRelation<E> {

    /**
     * Metadata about the index orders for this relation.
     */
    private static class KeyOrder extends AbstractKeyOrder<E> {

        public Comparator<E> getComparator() {
            return new EComparator();
        }

        public String getIndexName() {
            return "primary";
        }

        /**
         * There is only one component in the key.
         */
        public int getKeyArity() {
            return 2;
        }

        /**
         * The [name] and [value] attributes are used to generate the key.
         * [name] is at index zero in the key. [value] is at index 1.
         */
        public int getKeyOrder(final int keyPos) {
            if (keyPos < 0 || keyPos > 1)
                throw new IndexOutOfBoundsException();
            return keyPos;
        }

    }

    /**
     * The only defined index order (the primary key).
     */
    private static final KeyOrder primaryKeyOrder = new KeyOrder();

    /**
     * @param indexManager
     * @param namespace
     * @param timestamp
     * @param properties
     */
    public R(IIndexManager indexManager, String namespace, Long timestamp,
            Properties properties) {

        super(indexManager, namespace, timestamp, properties);

    }

    public Class<E> getElementClass() {

        return E.class;

    }

    public void create() {

        super.create();

        final IndexMetadata metadata = new IndexMetadata(
                getFQN(primaryKeyOrder), UUID.randomUUID());

        getIndexManager().registerIndex(metadata);

    }

    public void destroy() {

        // drop indices.
        for (String name : getIndexNames()) {

            getIndexManager().dropIndex(name);

        }

        super.destroy();

    }

//    public E newElement(final IPredicate<E> predicate,
//            final IBindingSet bindingSet) {
//
//        final String name = (String) predicate.asBound(0, bindingSet);
//
//        final String value = (String) predicate.asBound(1, bindingSet);
//
//        return new E(name, value);
//    }

    public E newElement(final List<IVariableOrConstant<?>> a,
            final IBindingSet bindingSet) {

        final String name = (String) a.get(0).get(bindingSet);

        final String value = (String) a.get(1).get(bindingSet);
        
        return new E(name,value);
        
    }
    
    public Set<String> getIndexNames() {

        final Set<String> tmp = new HashSet<String>();

        tmp.add(getFQN(primaryKeyOrder));

        return tmp;

    }

    public IKeyOrder<E> getPrimaryKeyOrder() {
        
        return primaryKeyOrder;
        
    }
    
    public IKeyOrder<E> getKeyOrder(final IPredicate<E> p) {

        return primaryKeyOrder;
        
    }

    /**
     * Simple insert procedure works fine for a local journal.
     */
    public long insert(final IChunkedOrderedIterator<E> itr) {

        long n = 0;

        final IIndex ndx = getIndex(primaryKeyOrder);

        final IKeyBuilder keyBuilder = ndx.getIndexMetadata().getKeyBuilder();
        
        while (itr.hasNext()) {

            final E e = itr.next();

            final byte[] key = primaryKeyOrder.getKey(keyBuilder, e);

            if (!ndx.contains(key)) {

                /*
                 * Note: The key is formed from both the name and the value.
                 * This makes it possible to have a many-to-many association
                 * pattern.
                 * 
                 * Note: The entire record is associated with the key, not just
                 * the value. This makes it possible for us to extract the
                 * record in cases where the key can not be decoded (such as
                 * Unicode key components).
                 */
                ndx.insert(key, e);

//                if (log.isTraceEnabled())
//                    log.trace
                    System.err.println("insert: element=" + e + ", key="
                            + BytesUtil.toString(key));
                
                n++;

            }

        }

        return n;

    }

    /**
     * Simple delete implementation works fine for a local journal.
     */
    public long delete(final IChunkedOrderedIterator<E> itr) {

        long n = 0;

        final IIndex ndx = getIndex(primaryKeyOrder);

        while (itr.hasNext()) {

            final E e = itr.next();

            if (ndx.remove(e.name) != null) {

                n++;

            }

        }

        return n;

    }

}
