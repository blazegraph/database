/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Jul 7, 2008
 */

package com.bigdata.rdf.spo;

import java.util.Arrays;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.AbstractKeyArrayIndexProcedure.ResultBitBufferHandler;
import com.bigdata.btree.BatchContains.BatchContainsConstructor;
import com.bigdata.rdf.spo.SPOConvertingIterator.SPOConverter;

/**
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BulkFilterConverter implements SPOConverter {

    private final IIndex ndx;

    private final boolean present;

    final SPOTupleSerializer tupleSer;
    
    /**
     * 
     * @param ndx
     *            The SPO index.
     * @param present
     *            true if true, filter for statements that exist in the db,
     *            otherwise filter for statements that do not exist
     */
    public BulkFilterConverter(IIndex ndx, boolean present) {
        
        this.ndx = ndx;
        
        this.present = present;
        
        tupleSer = (SPOTupleSerializer) ndx.getIndexMetadata()
                .getTupleSerializer();

    }

    public SPO[] convert(SPO[] chunk) {

        Arrays.sort(chunk, SPOComparator.INSTANCE);
                
//        // Thread-local key builder.
//        final RdfKeyBuilder keyBuilder = getKeyBuilder();
        
        // create an array of keys for the chunk
        final byte[][] keys = new byte[chunk.length][];
        
        for (int i = 0; i < chunk.length; i++) {
        
            keys[i] = tupleSer.statement2Key(SPOKeyOrder.SPO, chunk[i]);
            
        }

        // knows how to aggregate ResultBitBuffers.
        final ResultBitBufferHandler resultHandler = new ResultBitBufferHandler(
                keys.length);
        
        // submit the batch contains procedure to the SPO index
        ndx.submit(0/* fromIndex */, keys.length/* toIndex */, keys,
                null/* vals */, BatchContainsConstructor.INSTANCE,
                resultHandler);
        
        // get the array of existence test results
        boolean[] contains = resultHandler.getResult().getResult();
        
        // filter in or out, depending on the present variable
        int chunkSize = chunk.length;
        int j = 0;
        for (int i = 0; i < chunk.length; i++) {
            if (contains[i] == present) {
                chunk[j] = chunk[i];
                j++;
            } else {
                chunkSize--;
            }
        }

        // size the return array correctly
        SPO[] filtered = new SPO[chunkSize];
        
        System.arraycopy(chunk, 0, filtered, 0, chunkSize);
        
        return filtered;
        
    }

}
