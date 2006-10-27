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
