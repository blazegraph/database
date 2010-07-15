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
 * Created on Jul 8, 2008
 */

package com.bigdata.rdf.lexicon;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import junit.framework.TestCase2;
import com.bigdata.btree.BytesUtil;
import com.bigdata.io.SerializerUtil;
import com.bigdata.rdf.internal.TermId;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestId2TermTupleSerializer extends TestCase2 {

    /**
     * 
     */
    public TestId2TermTupleSerializer() {
    }

    /**
     * @param arg0
     */
    public TestId2TermTupleSerializer(String arg0) {
        super(arg0);
    }

    public void test_id2key() {
        
        final String namespace = "lexicon";
        
        final Id2TermTupleSerializer fixture = new Id2TermTupleSerializer(
                namespace, BigdataValueFactoryImpl.getInstance(namespace));

        TermId id1 = new TermId(VTE.URI, -1);
        TermId id2 = new TermId(VTE.URI, 0);
        TermId id3 = new TermId(VTE.URI, 1);

        byte[] k1 = fixture.id2key(id1);
        byte[] k2 = fixture.id2key(id2);
        byte[] k3 = fixture.id2key(id3);

        System.err
                .println("k1(termId:" + id1 + ") = " + BytesUtil.toString(k1));
        System.err
                .println("k2(termId:" + id2 + ") = " + BytesUtil.toString(k2));
        System.err
                .println("k3(termId:" + id3 + ") = " + BytesUtil.toString(k3));

        /*
         * Verify that ids assigned in sequence result in an order for the
         * corresponding keys in the same sequence.
         */
        assertTrue(BytesUtil.compareBytes(k1, k2) < 0);
        assertTrue(BytesUtil.compareBytes(k2, k3) < 0);

    }

    /**
     * A unit test of the proposal for introducing backward compatible
     * versioning into an unversioned class.
     */
    public void test_versionedSerialization() {
        
        final TestClass v0 = new TestClass((short) 0/* version */, "namespace",
                BigdataValueFactoryImpl.class.getName());

        final TestClass v1 = new TestClass((short) 1/* version */, "namespace",
                "valueFactoryClass");

//        final TestClass v2 = new TestClass((short) 2/* version */, "namespace",
//                "valueFactoryClass");

        assertEquals(v0, (TestClass)SerializerUtil.deserialize(SerializerUtil
                .serialize(v0)));

        assertEquals(v1, (TestClass)SerializerUtil.deserialize(SerializerUtil
                .serialize(v1)));
        
//        assertEquals(v2, SerializerUtil.deserialize(SerializerUtil
//                .serialize(v2)));
        
    }
    
    private void assertEquals(final TestClass expected, final TestClass actual) {
        assertEquals("version", expected.version, actual.version);
        assertEquals("namespace", expected.namespace, actual.namespace);
        assertEquals("valueFactoryClass", expected.valueFactoryClass,
                actual.valueFactoryClass);
    }

    /**
     * @todo test w/ and w/o a base class which implements
     *       {@link Externalizable}. It SHOULD not matter.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private static class V1 implements Externalizable {

        public void readExternal(ObjectInput in) throws IOException,
                ClassNotFoundException {
            
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            
        }
    }

    /**
     * A test class implementing {@link Externalizable} and incorporating the
     * proposed backward compatible introduction of versioning into the
     * {@link Id2TermTupleSerializer} class.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private static class TestClass
    // extends V2
    implements Externalizable {

        /**
         * Note: This field is unchanging. If it were to change then
         * deserialization of the class would break for previous versions.
         */
        private static final long serialVersionUID = 1L;

        /**
         * Included only the UTF serialization of the namespace field without
         * explicit version support.
         * 
         * <pre>
         * namespace:UTF
         * </pre>
         */
        static final transient short VERSION0 = 0;

        /**
         * Added the UTF serialization of the class name of the value factory
         * and an explicit version number in the serialization format. This
         * version is detected by a read of an empty string from the original
         * UTF field.
         * 
         * <pre>
         * "":UTF
         * valueFactoryClass:UTF
         * namespace:UTF
         * </pre>
         */
        static final transient short VERSION1 = 1;

//        static final transient short VERSION = VERSION2;

        String namespace;

        String valueFactoryClass;

        short version;

        /**
         * Deserialization constructor.
         */
        public TestClass() {
            
        }

        public TestClass(short version, String namespace,
                String valueFactoryClass) {
            this.version = version;
            this.namespace = namespace;
            this.valueFactoryClass = valueFactoryClass;
        }
        
        public void readExternal(ObjectInput in) throws IOException,
                ClassNotFoundException {
            short version = VERSION0;
            String s1 = in.readUTF();
            String s2 = BigdataValueFactoryImpl.class.getName();
            if (s1.length() == 0) {
                version = in.readShort();
                s1 = in.readUTF();
                s2 = in.readUTF();
            }
            final String namespace = s1;
            final String valueFactoryClass = s2;
            this.version = version;
            this.namespace = namespace;
            this.valueFactoryClass = valueFactoryClass;
            System.err.println("read: " + this);
        }

        public void writeExternal(ObjectOutput out) throws IOException {

            switch (version) {
            case VERSION0:
                out.writeUTF(namespace);
                break;
            case VERSION1:
                out.writeUTF("");
                out.writeShort(version);
                out.writeUTF(namespace);
                out.writeUTF(valueFactoryClass);
                break;
            default:
                throw new AssertionError();
            }
            System.err.println("wrote: "+this);
        }

        public String toString() {
            return "{version=" + version + ",namespace=" + namespace
                    + ",valueFactoryClass=" + valueFactoryClass + "}";
        }

    }

}
