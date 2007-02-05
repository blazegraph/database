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
package com.bigdata.io;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Reads bytes from a {@link ByteBuffer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo override {@link InputStream#read(byte[], int, int)} for better performance.
 */
public class ByteBufferInputStream extends InputStream {

    final ByteBuffer buf;

    public ByteBufferInputStream(ByteBuffer buf) {

        assert buf != null;

        this.buf = buf;

    }

    /**
     * Read the next byte from the buffer.
     * 
     * @return The byte as a value in [0:255].
     */
    public int read() throws IOException {

        if (buf.remaining() == 0) {

            return -1;

        }

        // A byte whose value is in [-128:127].
        byte b = buf.get();

        return (0xff & b);
        //            return ((int) b) + 128;
        //            int v = ((int)b) + 128;
        //            assert v>=0 && v<=255;
        //            return v;
        //            return b;

    }

}
