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
 * Created on Jan 7, 2008
 */

package com.bigdata.rdf.spo;

import com.bigdata.btree.AbstractIndexProcedureConstructor;
import com.bigdata.btree.AbstractKeyArrayIndexProcedure;
import com.bigdata.btree.IDataSerializer;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IParallelizableIndexProcedure;
import com.bigdata.rdf.inf.Justification;

/**
 * Procedure for writing {@link Justification}s on an index or index
 * partition.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class WriteJustificationsProc
        extends AbstractKeyArrayIndexProcedure
        implements IParallelizableIndexProcedure {

    /**
     * 
     */
    private static final long serialVersionUID = -7469842097766417950L;

    public final boolean isReadOnly() {
        
        return false;
        
    }
    
    /**
     * De-serialization constructor.
     *
     */
    public WriteJustificationsProc() {
        
        super();

    }

    public WriteJustificationsProc(IDataSerializer keySer, int fromIndex,
            int toIndex, byte[][] keys) {

        super(keySer, null, fromIndex, toIndex, keys, null/* vals */);

    }

    public static class WriteJustificationsProcConstructor extends
            AbstractIndexProcedureConstructor<WriteJustificationsProc> {

        public static WriteJustificationsProcConstructor INSTANCE = new WriteJustificationsProcConstructor();

        private WriteJustificationsProcConstructor() {
        }

        public WriteJustificationsProc newInstance(IDataSerializer keySer,
                IDataSerializer valSer, int fromIndex, int toIndex,
                byte[][] keys, byte[][] vals) {

            assert vals == null;

            return new WriteJustificationsProc(keySer, fromIndex, toIndex, keys);

        }

    }

    /**
     * @return The #of justifications actually written on the index as a
     *         {@link Long}.
     */
    public Object apply(IIndex ndx) {

        long nwritten = 0;
        
        final int n = getKeyCount();
        
        for (int i = 0; i < n; i++) {

            final byte[] key = getKey( i );
            
            if (!ndx.contains(key)) {

                ndx.insert(key, null/* no value */);

                nwritten++;

            }

        }
        
        return Long.valueOf(nwritten);
        
    }
    
}
