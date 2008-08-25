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
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure.ResultBuffer;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure.ResultBufferHandler;
import com.bigdata.btree.proc.BatchLookup.BatchLookupConstructor;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.SPOConvertingIterator.SPOConverter;

/**
 * Bulk completes the {@link StatementEnum} and/or statement identifier (SID)
 * for {@link ISPO}s using the given statement {@link IIndex}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BulkCompleteConverter implements SPOConverter {

    private final IIndex ndx;

    final SPOTupleSerializer tupleSer;
    
    /**
     * 
     * @param ndx
     *            The SPO index.
     */
    public BulkCompleteConverter(IIndex ndx) {
        
        this.ndx = ndx;
        
        tupleSer = (SPOTupleSerializer) ndx.getIndexMetadata()
                .getTupleSerializer();

    }

    public ISPO[] convert(ISPO[] chunk) {

        if (chunk == null)
            throw new IllegalArgumentException();
        
        Arrays.sort(chunk, SPOComparator.INSTANCE);

//        // Thread-local key builder.
//        final RdfKeyBuilder keyBuilder = getKeyBuilder();
        
        // create an array of keys for the chunk
        final byte[][] keys = new byte[chunk.length][];
        
        for (int i = 0; i < chunk.length; i++) {
            
            keys[i] = tupleSer.statement2Key(SPOKeyOrder.SPO, chunk[i]);
            
        }

        // knows how to aggregate ResultBitBuffers.
        final ResultBufferHandler resultHandler =
                new ResultBufferHandler(keys.length, ndx
                        .getIndexMetadata()
                        .getTupleSerializer()
                        .getLeafValueSerializer());
        
        // submit the batch contains procedure to the SPO index
        ndx.submit(
                0/* fromIndex */, keys.length/* toIndex */,
                keys, null/* vals */,
                BatchLookupConstructor.INSTANCE, resultHandler);
        
        // get the values from lookup
        
        final ResultBuffer vals = resultHandler.getResult();
        
        for (int i = 0; i < chunk.length; i++) {
            
            final byte[] val = vals.getResult(i);
            
            if (val != null) {
                
                SPO.decodeValue(chunk[i], val);
                
            } else {
                
                // the SPO does not actually exist in the db
                // let's make it inferred
                
                chunk[i].setStatementType(StatementEnum.Inferred);
                
            }
            
        }
        
        return chunk;
        
    }

}
