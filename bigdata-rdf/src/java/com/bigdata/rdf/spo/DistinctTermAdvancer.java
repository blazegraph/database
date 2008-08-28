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
 * Created on Aug 4, 2008
 */

package com.bigdata.rdf.spo;

import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleCursor;
import com.bigdata.btree.filter.Advancer;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.store.IRawTripleStore;

/**
 * Advances the source {@link ITupleCursor} through the distinct term
 * identifiers for some {@link SPOAccessPath}. Each time a new
 * {@link ITuple} is visited, the term identifier for the first position in
 * that tuple is decoded and its successor is formed. The source
 * {@link ITupleCursor} is then advanced to the key having that term
 * identifier in its first position and {@link IRawTripleStore#NULL} in its
 * 2nd and 3rd position. For example, if the {@link ITupleCursor} visits an
 * {@link ITuple} whose term identifiers are, in the order in which they
 * appear in the key:
 * 
 * <pre>
 * [ 12, 4, 44 ]
 * </pre>
 * 
 * Then the source {@link ITupleCursor} will be advanced to the key:
 * 
 * <pre>
 * [ 13, 0, 0 ]
 * </pre>
 * 
 * This is used to efficiently visit the distinct terms actually appearing
 * in the subject, predicate, or object position of {@link SPO}s in the
 * database.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DistinctTermAdvancer extends Advancer<SPO> {

    private static final long serialVersionUID = 2500001864793869957L;

    private transient KeyBuilder keyBuilder;

    public DistinctTermAdvancer() {

    }

    @Override
    protected void advance(ITuple<SPO> tuple) {

        if (keyBuilder == null) {

            /*
             * Note: It appears that you can not set this either implictly or
             * explicitly during ctor initialization if you want it to exist
             * during de-serialization. Hence it is initialized lazily here.
             * This is Ok since the iterator pattern is single threaded.
             */
            
            keyBuilder = new KeyBuilder(Bytes.SIZEOF_LONG * IRawTripleStore.N);

        }
        
        final long id = KeyBuilder
                .decodeLong(tuple.getKeyBuffer().array(), 0/* offset */);

        // restart scan at the next possible term id.
        final long nextId = id + 1;

        keyBuilder.reset().append(nextId);

        src.seek(keyBuilder.getBuffer());

    }

}
