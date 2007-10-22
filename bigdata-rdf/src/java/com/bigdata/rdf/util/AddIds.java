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
 * Created on May 21, 2007
 */
package com.bigdata.rdf.util;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndexWithCounter;
import com.bigdata.btree.IKeyBuffer;
import com.bigdata.btree.IValueBuffer;
import com.bigdata.btree.KeyBufferSerializer;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.btree.ValueBufferSerializer;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.service.IProcedure;

/**
 * Unisolated write operation makes consistent assertions on the
 * <em>ids</em> index based on the data developed by the {@link AddTerms}
 * operation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AddIds implements IProcedure, Externalizable {

    /**
     * 
     */
    private static final long serialVersionUID = 7387694802894116258L;
    
    private IKeyBuffer keys;
    private IValueBuffer vals;
    
    /**
     * De-serialization constructor.
     */
    public AddIds() {
        
    }
    
    public AddIds(IKeyBuffer keys,IValueBuffer vals) {

        assert keys != null;
        assert vals != null;
        assert keys.getKeyCount() == vals.getValueCount();
        
        this.keys = keys;
        this.vals = vals;
        
    }
    
    /**
     * Conditionally inserts each key-value pair into the index. The keys
     * are the term identifiers. The values are the terms as serialized by
     * {@link _Value#serialize()}. Since a conditional insert is used, the
     * operation does not cause terms that are already known to the ids
     * index to be re-inserted, thereby reducing writes of dirty index
     * nodes.
     * 
     * @param ndx
     *            The index.
     * 
     * @return <code>null</code>.
     */
    public Object apply(IIndexWithCounter ndx) throws Exception {

        final int n = keys.getKeyCount();

        for(int i=0; i<n; i++) {
    
            final byte[] key = keys.getKey(i);
            
            final byte[] val;

            /*
             * Note: Validation SHOULD be disabled except for testing.
             * 
             * FIXME turn off validation for release or performance testing.
             */
            final boolean validate = true; 
            
            if (validate) {

                /*
                 * When the term identifier is found in the reverse mapping
                 * this code path validates that the serialized term is the
                 * same.
                 */
                byte[] oldval = (byte[]) ndx.lookup(key);
                
                val = vals.getValue(i);
                
                if( oldval == null ) {
                    
                    if (ndx.insert(key, val) != null) {

                        throw new AssertionError();

                    }
                    
                } else {

                    /*
                     * Note: This would fail if the serialization of the
                     * term was changed. In order to validate when different
                     * serialization formats might be in use you have to
                     * actually deserialize the terms. However, I have the
                     * validation logic here just as a santity check while
                     * getting the basic system running - it is not meant to
                     * be deployed.
                     */

                    if (! BytesUtil.bytesEqual(val, oldval)) {

                        throw new RuntimeException(
                                "Consistency problem: id="
                                        + KeyBuilder.decodeLong(key, 0));
                        
                    }
                    
                }
                
            } else {
                
                /*
                 * This code path does not validate that the term identifier
                 * is mapped to the same term. This is the code path that
                 * you SHOULD use.
                 */

                if (!ndx.contains(key)) {

                    val = vals.getValue(i);
                    
                    if (ndx.insert(key, val) != null) {

                        throw new AssertionError();

                    }

                }

            }
            
        }
        
        return null;
        
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

        /*
         * Read the entire input stream into a buffer.
         */
        DataOutputBuffer buf = new DataOutputBuffer(in);
        
        /*
         * Unpack the buffer.
         */
        DataInputBuffer is = new DataInputBuffer(buf.buf,0,buf.len);
        
        keys = KeyBufferSerializer.INSTANCE.getKeys(is);

        vals = ValueBufferSerializer.INSTANCE.deserialize(is);
        
        assert keys.getKeyCount() == vals.getValueCount();

    }

    public void writeExternal(ObjectOutput out) throws IOException {

        /*
         * Setup a buffer for serialization.
         */
        DataOutputBuffer buf = new DataOutputBuffer();
        
        /*
         * Serialize the keys onto the buffer.
         */
        KeyBufferSerializer.INSTANCE.putKeys(buf, keys);
        
        /*
         * Serialize the values onto the buffer.
         * 
         * @todo Use suitable compression mode on the value buffer when
         * serialized.
         */
        ValueBufferSerializer.INSTANCE.serialize(buf, vals);
        
        /*
         * Copy the serialized form onto the caller's output stream.
         */
        out.write(buf.buf,0,buf.len);

    }

}
