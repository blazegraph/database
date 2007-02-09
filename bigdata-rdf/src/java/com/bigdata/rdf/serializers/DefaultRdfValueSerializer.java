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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.openrdf.model.Value;

import com.bigdata.objndx.IValueSerializer;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;

/**
 * Serializes the RDF {@link Value}s using default Java serialization. The
 * {@link _Value} class heirarchy implements {@link Externalizable} in order
 * to boost performance a little bit.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DefaultRdfValueSerializer implements IValueSerializer {

    private static final long serialVersionUID = 2393897553755023082L;
    
    public static transient final IValueSerializer INSTANCE = new DefaultRdfValueSerializer();
    
    public DefaultRdfValueSerializer() {}
    
    public void getValues(DataInputStream is, Object[] vals, int n)
            throws IOException {

        Object[] a = (Object[]) vals;
        
        ObjectInputStream ois = new ObjectInputStream(is);
        
        try {

            for (int i = 0; i < n; i++) {

                a[i] = ois.readObject();
            }
            
        } catch( Exception ex ) {
            
            IOException ex2 = new IOException();
            
            ex2.initCause(ex);
            
            throw ex2;
            
        }
        
    }

    public void putValues(DataOutputStream os, Object[] vals, int n)
            throws IOException {

        if (n == 0)
            return;

        Object[] a = (Object[]) vals;

        ObjectOutputStream oos = new ObjectOutputStream(os);

        for (int i = 0; i < n; i++) {

            oos.writeObject(a[i]);

        }
        
        oos.flush();

    }
    
}