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

import java.io.IOException;

import org.CognitiveWeb.extser.IExtensibleSerializer;

/**
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IOMExtensibleSerializer {

    /**
     * The object manager.
     * 
     * @return
     */
    public IOM getObjectManager();
    
    /**
     * @todo there is something NOT good about the interface hierarchy here. We
     *       have to declare these methods so that the
     *       {@link TxExtensibleSerializer} can expose them as well. However,
     *       the {@link OMExtensibleSerializer} is really using the methods
     *       declared by {@link IExtensibleSerializer}. This just looks bad in
     *       Java which is not known for letting you mask methods in multiple
     *       inheritance. I think that the answer lies in creating a divide in
     *       extser itself between the service (managing persistent state of the
     *       classIds and versioned serializers) and the local behavior
     *       (serializing and deserializing and caching the remote state).
     */
    public byte[] serialize( long oid, Object obj )
    throws IOException;
    
    public Object deserialize( long oid, byte[] serialized )
    throws IOException;

}
