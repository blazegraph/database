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
 * Created on May 7, 2007
 */

package com.bigdata.rdf.model;

import java.io.Externalizable;
import java.util.UUID;

import junit.framework.TestCase2;

import org.openrdf.vocabulary.XmlSchema;

import com.bigdata.io.SerializerUtil;
import com.bigdata.rdf.model.OptimizedValueFactory._BNode;
import com.bigdata.rdf.model.OptimizedValueFactory._Literal;
import com.bigdata.rdf.model.OptimizedValueFactory._URI;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSerialization extends TestCase2 {

    /**
     * 
     */
    public TestSerialization() {
    }

    /**
     * @param arg0
     */
    public TestSerialization(String arg0) {
        super(arg0);
    }

    /**
     * Performs round trip using the {@link Externalizable} implementation on
     * {@link _Value}.
     * 
     * @param o
     *            The {@link _Value}.
     *            
     * @return The de-serialized {@link _Value}.
     */
    protected _Value roundTrip_externalizable(_Value o) {
        
        return (_Value) SerializerUtil.deserialize(SerializerUtil.serialize(o));
        
    }
    
    /**
     * Performs round trip (de-)serialization using {@link _Value#serialize()}
     * and {@link _Value#deserialize(byte[])}.
     * 
     * @param o The {@link _Value}
     * 
     * @return The de-serialized {@link _Value}.
     */
    protected _Value roundTrip_tuned(_Value o) {
        
        return _Value.deserialize(o.serialize());
        
    }
    
    /**
     * Test round trip of some URIs.
     */
    public void test_URIs() {

        _URI a = new _URI("http://www.bigdata.com");
        
        assertEquals(a, roundTrip_externalizable(a));
        assertEquals(a, roundTrip_tuned(a));
        
    }
    
    /**
     * Test round trip of some plain literals.
     */
    public void test_plainLiterals() {

        _Literal a = new _Literal("bigdata");
        
        assertEquals(a, roundTrip_externalizable(a));
        assertEquals(a, roundTrip_tuned(a));
        
    }
    
    /**
     * Test round trip of some language code literals.
     */
    public void test_langCodeLiterals() {

        _Literal a = new _Literal("bigdata","en");
        
        assertEquals(a, roundTrip_externalizable(a));
        assertEquals(a, roundTrip_tuned(a));
        
    }

    /**
     * Test round trip of some datatype literals.
     */
    public void test_dataTypeLiterals() {

        _Literal a = new _Literal("bigdata",new _URI(XmlSchema.INT));
        
        assertEquals(a, roundTrip_externalizable(a));
        assertEquals(a, roundTrip_tuned(a));
        
    }

    /**
     * Test round trip of some bnodes.
     */
    public void test_bnodes() {

        _BNode a = new _BNode(UUID.randomUUID().toString());
        
        assertEquals(a, roundTrip_externalizable(a));
        assertEquals(a, roundTrip_tuned(a));
        
    }

}
