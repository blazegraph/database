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
 * Created on Sep 5, 2007
 */

package com.bigdata.rawstore;

import java.nio.ByteBuffer;

/**
 * Abstract base class for {@link IRawStore} implementations. This class uses a
 * delegation pattern for the {@link IStoreSerializer} interface and does not
 * implement either the methods defined directly by the {@link IRawStore}
 * interface nor the methods of the {@link IAddressManager} interface. As such
 * it may be used as an abstract base class by any {@link IRawStore}
 * implementation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractRawStore implements IRawStore, IStoreSerializer {

    /**
     * The object that handles serialization.
     */
    protected final IStoreSerializer serializer;
    
    /**
     * The designated constructor.
     */
    public AbstractRawStore() {

        serializer = new StoreSerializer(this);
        
    }

    final public Object deserialize(byte[] b, int off, int len) {
        
        return serializer.deserialize(b, off, len);
        
    }

    final public Object deserialize(byte[] b) {
        
        return serializer.deserialize(b,0,b.length);
        
    }

    final public Object deserialize(ByteBuffer buf) {
        
        return serializer.deserialize(buf);
        
    }

    final public byte[] serialize(Object obj) {

        return serializer.serialize(obj);
        
    }

}
