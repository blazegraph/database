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
 * Created on Aug 4, 2008
 */

package com.bigdata.rdf.spo;

import com.bigdata.btree.ITuple;
import com.bigdata.btree.filter.Advancer;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.keys.SuccessorUtil;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.striterator.IKeyOrder;

/**
 * This was cloned from the {@link DistinctTermAdvancer}. It supports an
 * efficient scan of the distinct term identifiers that appear in the first
 * position(s) of the keys for the statement index corresponding to the
 * specified {@link IKeyOrder}. For example, using {@link SPOKeyOrder#POS} will
 * give you the term identifiers for the distinct predicates actually in use
 * within statements in the {@link SPORelation}.
 * <p>
 * Note: This class only offers additional functionality over the
 * {@link DistinctTermAdvancer} for a quad store. For example, consider a triple
 * store with 2-bound on the {@link SPOKeyOrder#SPO} index. SInce you are only
 * going to visit the distinct Object values, the advancer will not "advance"
 * over anything and you might as well use a normal {@link IAccessPath} or
 * rangeIterator.
 * 
 * @see DistinctTermAdvancer
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: DistinctMultiTermAdvancer.java 2886 2010-05-19 19:16:49Z
 *          mroycsi $
 * 
 * @todo Unit tests?
 */
public class DistinctMultiTermAdvancer extends Advancer<SPO> {

//    private static final long    serialVersionUID = 2500001864793869957L;

    /**
     * New version for this class, which  
     */
    private static final long serialVersionUID = -7326621294779476500L;
    
    private final int arity;
    private final int boundEntries;

    private transient IKeyBuilder keyBuilder;

    public DistinctMultiTermAdvancer(final int arity, final int boundEntries) {

        this.arity = arity;
        this.boundEntries = boundEntries;
    }

    @Override
    protected void advance(final ITuple<SPO> tuple) {

        if (keyBuilder == null) {

            /*
             * Note: It appears that you can not set this either implicitly or
             * explicitly during ctor initialization if you want it to exist
             * during de-serialization. Hence it is initialized lazily here.
             * This is Ok since the iterator pattern is single threaded.
             */

            keyBuilder = KeyBuilder.newInstance();

        }

        final byte[] key = tuple.getKey();
        
        final IV[] ivs = IVUtility.decode(key, boundEntries+1);
        
        keyBuilder.reset();
        
        for (int i = 0; i < ivs.length; i++) {
            ivs[i].encode(keyBuilder);
        }
        
        final byte[] fromKey = keyBuilder.getKey();
        
        final byte[] toKey = SuccessorUtil.successor(fromKey.clone());

        src.seek(toKey);

    }

}
