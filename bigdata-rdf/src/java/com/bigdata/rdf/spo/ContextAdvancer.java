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
 * Created on Oct 1, 2010
 */

package com.bigdata.rdf.spo;

import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleCursor;
import com.bigdata.btree.filter.Advancer;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.keys.SuccessorUtil;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;

/**
 * Advancer for a quads index whose last key component is the "context position
 * (such as SPOC or SOPC). The advancer will skip to first possible key for the
 * next distinct triple for each quad which it visits. This is a cheap way to
 * impose a "DISTINCT" filter using an index scan and works well for both local
 * and scale-out indices.
 * <p>
 * You have to use {@link IRangeQuery#CURSOR} to request an {@link ITupleCursor}
 * when using an {@link Advancer} pattern.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ContextAdvancer extends Advancer<SPO> {

    private static final long serialVersionUID = 1L;

    private transient IKeyBuilder keyBuilder;

    public ContextAdvancer() {
        
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

        // extract the key.
        final byte[] key = tuple.getKey();
        
        // decode the first three components of the key.
        final IV[] terms = IVUtility.decode(key, 3/*nterms*/);
        
        // reset the buffer.
        keyBuilder.reset();

        // encode the first three components of the key.
        IVUtility.encode(keyBuilder,terms[0]);
        IVUtility.encode(keyBuilder,terms[1]);
        IVUtility.encode(keyBuilder,terms[2]);
        
        // obtain the key.
        final byte[] fromKey = keyBuilder.getKey();
        
        // obtain the successor of the key.
        final byte[] toKey = SuccessorUtil.successor(fromKey.clone());

        // seek to that successor.
        src.seek(toKey);
        
    }

}
