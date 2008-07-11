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
 * Created on May 7, 2007
 */

package com.bigdata.rdf.lexicon;

import java.io.Externalizable;
import java.util.UUID;

import junit.framework.TestCase2;

import org.openrdf.model.vocabulary.XMLSchema;

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

        _Literal a = new _Literal("bigdata",new _URI(XMLSchema.INT));
        
        assertEquals(a, roundTrip_externalizable(a));
        assertEquals(a, roundTrip_tuned(a));
        
    }

    /*
     * Note: BNode serialization has been disabled since we never write
     * them on the database.
     */
//    /**
//     * Test round trip of some bnodes.
//     */
//    public void test_bnodes() {
//
//        _BNode a = new _BNode(UUID.randomUUID().toString());
//        
//        assertEquals(a, roundTrip_externalizable(a));
//        assertEquals(a, roundTrip_tuned(a));
//        
//    }

}
