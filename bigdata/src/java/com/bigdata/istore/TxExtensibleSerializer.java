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
 * Created on Oct 27, 2006
 */

package com.bigdata.istore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.CognitiveWeb.extser.AbstractExtensibleSerializer;
import org.CognitiveWeb.extser.ISerializer;

import com.bigdata.istore.OMExtensibleSerializer.MyDataInputStream;
import com.bigdata.istore.OMExtensibleSerializer.MyDataOutputStream;

/**
 * Flyweight implementation delegates most operations but ensures that the
 * application sees the transactionally isolated object manager instance rather
 * than the global object manager instance during deserialization.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TxExtensibleSerializer extends
        AbstractExtensibleSerializer implements IOMExtensibleSerializer {

    /**
     * Note: Instances of this class do NOT get persisted directly. 
     */
    private static final long serialVersionUID = 1L;
    
    private final ITx tx;
    private final OMExtensibleSerializer delegate;

    /**
     * This will be a transactional object manager.
     */
    public IOM getObjectManager() {

        return tx;

    }

    public TxExtensibleSerializer(ITx tx, OMExtensibleSerializer delegate ) {

        assert tx != null;
        
        assert delegate != null;
        
        this.tx = tx;
        
        this.delegate = delegate;

    }

    protected void update() {
        
        delegate.update();
        
    }

    /**
     * This will use the transactional object manager.
     */
    public DataOutputStream getDataOutputStream(long recid,
            ByteArrayOutputStream baos) throws IOException {
        return new MyDataOutputStream(recid, this, baos);
    }

    /**
     * This will use the transactional object manager.
     */
    public DataInputStream getDataInputStream(long recid,
            ByteArrayInputStream bais) throws IOException {
        return new MyDataInputStream(recid, this, bais);
    }

    /**
     * This will use the transactional object manager.
     */
    public ISerializer getSerializer(long oid) {

        return (ISerializer) tx.read(oid);
        
    }

}
