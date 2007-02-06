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
package com.bigdata.rdf;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.openrdf.model.Statement;

import com.bigdata.objndx.BTree;
import com.bigdata.objndx.BTreeMetadata;
import com.bigdata.objndx.IValueSerializer;
import com.bigdata.rawstore.IRawStore;

/**
 * A persistent index for RDF {@link Statement}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class StatementIndex extends BTree {

    public final KeyOrder keyOrder;
    
    /**
     * Create a new statement index.
     * 
     * @param store
     *            The backing store.
     */
    public StatementIndex(IRawStore store,KeyOrder keyOrder) {
        
        super(store, DEFAULT_BRANCHING_FACTOR, ValueSerializer.INSTANCE);
        
        assert keyOrder != null;
        
        this.keyOrder = keyOrder;
        
    }
    
    /**
     * Load a statement index from the store.
     * 
     * @param store
     *            The backing store.
     * @param metadataId
     *            The metadata record identifier for the index.
     */
    public StatementIndex(IRawStore store, long metadataId,KeyOrder keyOrder) {

        super(store, BTreeMetadata.read(store, metadataId));
        
        this.keyOrder = keyOrder;
        
    }

    /**
     * Note: There is no additional data serialized with a statement at this
     * time so the value serializer is essentially a nop. All the information is
     * in the keys.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class ValueSerializer implements IValueSerializer {

        private static final long serialVersionUID = -2174985132435709536L;

        public static transient final IValueSerializer INSTANCE = new ValueSerializer();
        
        public ValueSerializer(){}
        
        public void getValues(DataInputStream is, Object[] values, int n) throws IOException {
            return;
        }

        public void putValues(DataOutputStream os, Object[] values, int n) throws IOException {
            return;
        }
        
    }
    
}
