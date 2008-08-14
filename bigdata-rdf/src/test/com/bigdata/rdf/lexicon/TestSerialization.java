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

import junit.framework.TestCase2;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.XMLSchema;

import com.bigdata.rdf.model.BigdataValueSerializer;

/**
 * Test of the {@link BigdataValueSerializer}.
 * 
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
     * Fixture under test.
     */
    protected final BigdataValueSerializer<Value> fixture = new BigdataValueSerializer<Value>(
            ValueFactoryImpl.getInstance());
    
    /**
     * Performs round trip using the {@link Externalizable} implementation on
     * {@link Value}.
     * 
     * @param o
     *            The {@link Value}.
     * 
     * @return The de-serialized {@link Value}.
     * 
     * @deprecated The {@link Externalizable} iface is disabled since we can't
     *             write a stand-off deserializer for {@link Externalizable}.
     */
    protected Value roundTrip_externalizable(Value o) {
        
        // Note: Externalizable is disabled. 
        
        return o;
        
//        return (_Value) SerializerUtil.deserialize(SerializerUtil.serialize(o));
        
    }
    
    /**
     * Performs round trip (de-)serialization using {@link BigdataValueSerializer#serialize()}
     * and {@link BigdataValueSerializer#deserialize(byte[])}.
     * 
     * @param o The {@link Value}
     * 
     * @return The de-serialized {@link Value}.
     */
    protected Value roundTrip_tuned(Value o) {
        
        return fixture.deserialize(fixture.serialize(o));
        
    }
    
    /**
     * Test round trip of some URIs.
     */
    public void test_URIs() {

        final URI a = new URIImpl("http://www.bigdata.com");
        
//        assertEquals(a, roundTrip_externalizable(a));
        assertEquals(a, roundTrip_tuned(a));
        
    }
    
    /**
     * Test round trip of some plain literals.
     */
    public void test_plainLiterals() {

        final Literal a = new LiteralImpl("bigdata");
        
//        assertEquals(a, roundTrip_externalizable(a));
        assertEquals(a, roundTrip_tuned(a));
        
    }
    
    /**
     * Test round trip of some language code literals.
     */
    public void test_langCodeLiterals() {

        final Literal a = new LiteralImpl("bigdata","en");
        
//        assertEquals(a, roundTrip_externalizable(a));
        assertEquals(a, roundTrip_tuned(a));
        
    }

    /**
     * Test round trip of some datatype literals.
     */
    public void test_dataTypeLiterals() {

        final Literal a = new LiteralImpl("bigdata", XMLSchema.INT);
        
//        assertEquals(a, roundTrip_externalizable(a));
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
