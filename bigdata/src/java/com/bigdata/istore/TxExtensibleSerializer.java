/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
