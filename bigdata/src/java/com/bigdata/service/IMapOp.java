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
package com.bigdata.service;

import com.bigdata.btree.BytesUtil;

/**
 * Interface for client-defined mapped operators.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IMapOp {
    
    /**
     * The hash function used to assign map output keys to reduce tasks. This is
     * normally <code>hash(key) mod R</code>, where hash(key) is
     * {@link BytesUtil#hash(byte[])} and R is the #of reduce tasks.
     * 
     * @param key
     * @return
     */
    public int reduceHashCode(byte[] key);
    
    /**
     * Each map task will be presented with key-value pairs. When the source is
     * an index, the key-value pairs will be presented in key order. The map
     * operator is responsible for writting zero or more key value pairs on the
     * output sink. Those key value pairs will be assigned to N different reduce
     * tasks by applying the user-defined hash function to the output key.
     * 
     * @param key
     *            The input key.
     * @param val
     *            The input value.
     * @param out
     *            The output sink.
     */
    public void map(byte[] key, byte[] val, IOutput out);

    /**
     * Each reduce task will be presented with a series of key-value pairs in
     * key order. However, the keys will be distributed across the N reduce
     * tasks by the used defined hash function, so this is NOT a total ordering
     * over the intermediate keys.
     * 
     * @param key
     * @param val
     */
    public void reduce(byte[] key, byte[] val);
    
    public static interface IOutput {
        
        public void append(byte[] key,byte[] val);
        
    }
}
