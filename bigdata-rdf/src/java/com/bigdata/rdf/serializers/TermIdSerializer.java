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

import org.CognitiveWeb.extser.LongPacker;

import com.bigdata.btree.IValueSerializer;
import com.bigdata.io.DataOutputBuffer;

/**
 * The value is a <code>long</code> integer that is the term identifier.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 * @version $Id$
 */
public class TermIdSerializer implements IValueSerializer {

    private static final long serialVersionUID = 8081006629809857019L;
    
    public static transient final IValueSerializer INSTANCE = new TermIdSerializer();
    
    /**
     * Note: It is faster to use packed longs, at least on write with test
     * data (bulk load of wordnet nouns).
     */
    final static boolean packedLongs = true;
    
    public TermIdSerializer() {}
    
    public void getValues(DataInput is, Object[] values, int n)
            throws IOException {

        for(int i=0; i<n; i++) {
            
            if (packedLongs) {

                values[i] = Long.valueOf(LongPacker.unpackLong(is));

            } else {

                values[i] = Long.valueOf(is.readLong());

            }
            
        }
        
    }

    public void putValues(DataOutputBuffer os, Object[] values, int n)
            throws IOException {

        for(int i=0; i<n; i++) {

            if(packedLongs) {

//                LongPacker.packLong(os, ((Long) values[i]).longValue());
                os.packLong(((Long) values[i]).longValue());
                
            } else {

                os.writeLong(((Long) values[i]).longValue());
            
            }
            
        }
        
    }
    
}