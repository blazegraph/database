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
package com.bigdata.rdf.serializers;

import java.io.DataInput;
import java.io.IOException;

import com.bigdata.objndx.DataOutputBuffer;
import com.bigdata.objndx.IValueSerializer;

/**
 * Note: There is no additional data serialized with a statement at this time so
 * the value serializer is essentially a nop. All the information is in the
 * keys.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class StatementSerializer implements IValueSerializer {

    private static final long serialVersionUID = -2174985132435709536L;

    public static transient final IValueSerializer INSTANCE = new StatementSerializer();

    public StatementSerializer() {
    }

    public void getValues(DataInput is, Object[] values, int n)
            throws IOException {
        return;
    }

    public void putValues(DataOutputBuffer os, Object[] values, int n)
            throws IOException {
        return;
    }

}
