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
 * Created on Jun 13, 2006
 */

package org.CognitiveWeb.bigdata;

import java.util.Random;

import junit.framework.TestCase;

/**
 * Test suite for {@link Oid} focuses on the correctness of the bit math
 * operations.
 * 
 * @todo Are there ways to simplify the logic used to compute the relative and
 *       absolute oids?
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson
 *         </a>
 * @version $Id$
 */

public class TestOidMath extends TestCase {

    /**
     * 
     */
    public TestOidMath() {
        super();
    }

    /**
     * @param arg0
     */
    public TestOidMath(String arg0) {
        super(arg0);
    }

    /**
     * Many of these tests methods are stress tests using random number
     * generators to generate synthetic values. This field gives the number of
     * iterations of the stress test that will be run. Higher values provide
     * more coverage of the possible range of the methods under test.
     */
    final int LIMIT = 1000000; // 1M values.

    /**
     * Random number generator used in the stress tests.
     */
    Random rnd = new Random();
    
    /**
     * Verifies that {@link OId#getComponents(long)}and
     * {@link OId#getOid(short, short, short, short)}are interoperable.
     */
    public void test_components() {

        final int limit = LIMIT;
        
        final short[] components = new short[4];
        
        for( int i=0; i<limit; i++ ) {
            
            long expectedOid = nextNonZeroOid();

            OId.getComponents(expectedOid,components);
            
            long actualOid = OId.getOid(components[0], components[1],
                    components[2], components[3]);

//            System.out.println("i=" + i + ", expected="
//                    + Long.toHexString(expectedOid) + expectedOid + ", actual="
//                    + Long.toHexString(actualOid));

            assertEquals("oid",expectedOid,actualOid);
            
        }
                
    }

    /**
     * Test suite generates relative oids from absolute oids plus a context and
     * verifies that the relative oid can be used to reconstruct the absolute
     * oid. Random contexts are generated. The context and the absolute oid will
     * have 0-3 components of shared context. 0 shared components means that
     * they will be in different partitions of the distributed datbase. 1 shared
     * component means that they will be in the same partition in the database.
     * 2 shared components means that they will be in the same segment in the
     * database. 3 share components means that they will be on the same page in
     * the segment. Absolute oids are then produced by randomizing the
     * partition, segment and page component of the context as appropriate.
     */

    public void test_relativeOid() {
    
        final int limit = LIMIT;
        
        for( int i=0; i<limit; i++ ) {
            
            // context is random 64-bit integer.
            final long contextOid = nextNonZeroOid();

            // break down context into oid components.
            short[] components = OId.getComponents(contextOid);

            // #of oid components that will be shared.
            final int sharedComponents = rnd.nextInt(3);
            
            // always randomize the logical slot.
            components[ 3 ] = nextComponent();
            if( sharedComponents < 3 ) {
                // different page
                components[ 2 ] = nextComponent();
            }
            if( sharedComponents < 2 ) {
                // different segment
                components[ 1 ] = nextComponent();                
            }
            if( sharedComponents < 1 ) {
                // different partition
                components[ 0 ] = nextComponent();                
            }

            /*
             * Note: We explictly mask off the 16 high order bits to remove the
             * impact of sign extension when promoting the short component to an
             * int in order to display its hex value string.
             */
//            System.out.println("i=" + i + " #shared="
//                    + sharedComponents + ", P="
//                    + Integer.toHexString(0x0000ffff & components[0]) + ", S="
//                    + Integer.toHexString(0x0000ffff & components[1]) + ", p="
//                    + Integer.toHexString(0x0000ffff & components[2]) + ", s="
//                    + Integer.toHexString(0x0000ffff & components[3]));

            /*
             * Generate an absolute oid that may use randomized components for
             * the partition, partition + segment, or partition + segment +
             * page. If none of those components were randomized then the
             * absolute oid will differ only in the slot.
             */
            final long expectedAbsoluteOid = OId.getOid(components[0],
                    components[1], components[2], components[3]);

            doRelativeOidRoundTripTest( contextOid, expectedAbsoluteOid );
        
        }
        
    }

    /**
     * Tests for fence posts when a component of the oid is zero.
     */
    public void test_relativeOidWithZeroComponent() {

        if (true) {

            // all components random.
            
            short[] components = new short[4];

            components[0] = nextComponent(); // 16 random bits: P
            components[1] = nextComponent(); // 16 random bits: S
            components[2] = nextComponent(); // 16 random bits: p
            components[3] = nextComponent(); // 16 random bits: s

        }
        
        if (true) {

            // partition is zero.
            
            short[] components = new short[4];

            components[0] = 0; // nextComponent(); 16 random bits: P
            components[1] = nextComponent(); // 16 random bits: S
            components[2] = nextComponent(); // 16 random bits: p
            components[3] = nextComponent(); // 16 random bits: s

        }
        
        if (true) {

            // segment is zero.
            
            short[] components = new short[4];

            components[0] = nextComponent(); // 16 random bits: P
            components[1] = 0; // nextComponent(); // 16 random bits: S
            components[2] = nextComponent(); // 16 random bits: p
            components[3] = nextComponent(); // 16 random bits: s

        }
        
        if (true) {
            
            // page is zero.

            short[] components = new short[4];

            components[0] = nextComponent(); // 16 random bits: P
            components[1] = nextComponent(); // 16 random bits: S
            components[2] = 0; // nextComponent(); // 16 random bits: p
            components[3] = nextComponent(); // 16 random bits: s

        }
        
        if (true) {

            // slot is zero.
            
            short[] components = new short[4];

            components[0] = nextComponent(); // 16 random bits: P
            components[1] = nextComponent(); // 16 random bits: S
            components[2] = nextComponent(); // 16 random bits: p
            components[3] = 0; // nextComponent(); // 16 random bits: s
            
        }

        if (true) {

            // partition + segment are zero.
            
            short[] components = new short[4];

            components[0] = 0; // nextComponent(); // 16 random bits: P
            components[1] = 0; // nextComponent(); // 16 random bits: S
            components[2] = nextComponent(); // 16 random bits: p
            components[3] = nextComponent(); // 16 random bits: s

        }

        if (true) {

            // partition + segment + page are zero.
            
            short[] components = new short[4];

            components[0] = 0; // nextComponent(); // 16 random bits: P
            components[1] = 0; // nextComponent(); // 16 random bits: S
            components[2] = 0; // nextComponent(); // 16 random bits: p
            components[3] = nextComponent(); // 16 random bits: s

        }

        if (true) {

            // partition + segment + page + slot are zero.
            
            short[] components = new short[4];

            components[0] = 0; // nextComponent(); // 16 random bits: P
            components[1] = 0; // nextComponent(); // 16 random bits: S
            components[2] = 0; // nextComponent(); // 16 random bits: p
            components[3] = 0; // nextComponent(); // 16 random bits: s

        }

    }
    
    /**
     * Test verifies absolute to relative oid round trip when the context oid is
     * same same as the absolute oid.
     */

    public void test_relativeOidSameOid() {
        
        final int limit = LIMIT;

        for( int i=0; i<limit; i++ ) {
        
            long oid = nextNonZeroOid();
            
            doRelativeOidRoundTripTest(oid,oid);
        
        }
        
    }
   
    /**
     * Test verifies absolute to relative oid round trip when the absolute oid
     * is 0L ( a null reference ) for a variety of contexts.
     */
    public void test_relativeOidNullOid() {
        
        final int limit = LIMIT;

        for( int i=0; i<limit; i++ ) {
        
            long oid = nextNonZeroOid();
            
            doRelativeOidRoundTripTest(oid,0L);
        
        }
        
    }

    /**
     * Test when the absolute oid is on the same page as the context oid.
     */
    public void test_relativeOidSamePage() {

        final int limit = LIMIT;

        final short[] components = new short[4];
        
        for (int j = 0; j < limit; j++) {

            // Random context oid.
            long context = nextNonZeroOid();

//            /*
//             * For all possible slot values.
//             */
//            for (int i = Short.MIN_VALUE; i < Short.MAX_VALUE; i++) {
//
//                if (i == 0) {
//
//                    continue; // skip zero for slot component.
//
//                }

                OId.getComponents(context,components);

                components[3] = nextComponent();

                long oid = OId.getOid(components[0], components[1],
                        components[2], components[3]);

                doRelativeOidRoundTripTest(context, oid);

//            }

        }

    }
    
    /**
     * Test helper.
     */
    public void doRelativeOidRoundTripTest( long contextOid, long expectedAbsoluteOid )
    {
        /*
         * Convert the absolute oid into a oid that is relative to the
         * context oid.
         */
        final long relativeOid = OId.getRelativeOid(contextOid, expectedAbsoluteOid);

        /*
         * Convert the relative oid back into an absolute oid.
         */
        final long actualAbsoluteOid = OId.getAbsoluteOid(contextOid, relativeOid);
        
//        System.out.println("context=" + Long.toHexString(contextOid)
//                + ", expectedAbs=" + Long.toHexString(expectedAbsoluteOid)
//                + ", relativeOId=" + Long.toHexString(relativeOid)
//                + ", actualAbsOid=" + Long.toHexString(actualAbsoluteOid));

        /*
         * Verify that the absolute oid was correctly round tripped.
         */
        assertEquals("absoluteOid (context=" + contextOid + ")",
                expectedAbsoluteOid, actualAbsoluteOid);       
        
    }

    /**
     * Problem identified during stress test. The partition and segment are the
     * same. The page differs and the slot is zero for the absolute oid. The
     * code used to process each component independently, which means that it
     * was interpreting the zero slot identifier as being elided and replacing
     * it with the slot identifier from the context. The fix was to cascade so
     * that the zero slot identifier was not considered after the page
     * identifier was already known to be different.
     */
    public void test_weirdRelativeOidProblem() {
        long context = 0xd896be20abe47d1aL;
        long expectedAbsoluteOid = 0xd896be208b8e0000L;
        try {
            doRelativeOidRoundTripTest(context, expectedAbsoluteOid);
            fail("Expecting: "+IllegalArgumentException.class);
        }
        catch(IllegalArgumentException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }
    }

    /**
     * Problem identified during stress test. The parition and segment are the
     * same. The page is zero(0) for the absolute oid and the slots differ. The
     * issue is that the zero page identifier can not be differentiated from an
     * elided page identifier and the page identifier from the context is used
     * instead. The solution was to reject object identifiers having zero in one
     * or more of their components as illegal unless all components are zero.
     * For example, (4-0-5-1) is illegal but (0-0-0-0) is legal.
     */
    
    public void test_weirdRelativeOidProblem2() {
        long context = -2122258253775228049L;
        long expectedAbsoluteOid = -2122258257098570793L;
        try {
            doRelativeOidRoundTripTest(context, expectedAbsoluteOid);
            fail("Expecting: "+IllegalArgumentException.class);
        }
        catch(IllegalArgumentException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }
    }

    /**
     * Verify the method that masks off the page and slot components of an
     * object identifier.
     */
    public void test_maskPageAndSlot() {

        if( true ) {
            
            long oid      = 0x1004c0000L;
            long expected = 0x100000000L;

            assertEquals(expected,OId.maskPageAndSlot( oid ));
            
        }
        
        for (int i = 0; i < LIMIT; i++) {

            /*
             * Test with a random non-NULL object identifier.
             */
            
            long oid = nextNonZeroOid();

            short[] c = OId.getComponents(oid);

            long expected_oid = OId.getOid(c[0], c[1], (short) 0, (short) 0);

            long actual_oid = OId.maskPageAndSlot(expected_oid);

            assertEquals("expecting=" + new OId(expected_oid) + ", but found="
                    + new OId(actual_oid), expected_oid, actual_oid);

        }

        if( true ) {
            
            /*
             * Test with a NULL.
             */
            
            long oid = 0L;

            short[] c = OId.getComponents(oid);

            long expected_oid = OId.getOid(c[0], c[1], (short) 0, (short) 0);

            long actual_oid = OId.maskPageAndSlot(expected_oid);

            assertEquals("expecting=" + expected_oid + ", but found="
                    + actual_oid, expected_oid, actual_oid);
            
        }
        
    }
    
    /**
     * Test verifies that {@link OId#toString()} and {@link OId#OId(String)}
     * will roundtrip.
     */
    public void test_toString() {
        
        for (int i = 0; i < LIMIT; i++) {

            /*
             * Test with a random non-NULL object identifier.
             */
            
            long expected_oid = nextNonZeroOid();

            String str = new OId(expected_oid).toString();
            
            long actual_oid = new OId(str).toLong();
            
            assertEquals("expecting=" + new OId(expected_oid) + ", but found="
                    + new OId(actual_oid), expected_oid, actual_oid);

        }

        if( true ) {
            
            /*
             * Test with a NULL.
             */
            
            long expected_oid = 0L;

            String str = new OId(expected_oid).toString();
            
            long actual_oid = new OId(str).toLong();
            
            assertEquals("expecting=" + new OId(expected_oid) + ", but found="
                    + new OId(actual_oid), expected_oid, actual_oid);

        }
        
    }

    /**
     * Verify that the {@link OId#OId(int, int, int, int)} correctly detects and
     * reports when a component value exceeds the legal range for an unsigned
     * short. Both values that are too large (greated than 65535) and negative
     * values are tested for correct rejection.
     */
    public void test_ctorLimits() {

        /*
         * Four random components, but mask off the high half to defeat signed
         * promotion to an [int].
         */
        int P = nextComponent() & 0x0000ffff;
        int S = nextComponent() & 0x0000ffff;
        int p = nextComponent() & 0x0000ffff;
        int s = nextComponent() & 0x0000ffff;
        
        // correct acceptance test.
        new OId(P,S,p,s);

        //
        // negative values are rejected.
        //
        
        // partition
        try {
            new OId(-1,S,p,s);
            fail("Expecting: "+IllegalArgumentException.class);
        }
        catch(IllegalArgumentException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }

        // segment
        try {
            new OId(P,-1,p,s);
            fail("Expecting: "+IllegalArgumentException.class);
        }
        catch(IllegalArgumentException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }

        // page
        try {
            new OId(P,S,-1,s);
            fail("Expecting: "+IllegalArgumentException.class);
        }
        catch(IllegalArgumentException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }

        // slot.
        try {
            new OId(P,S,p,-1);
            fail("Expecting: "+IllegalArgumentException.class);
        }
        catch(IllegalArgumentException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }

        //
        // component is too large by one.
        //
        final int err = 1<<16;

        // partition.
        try {
            new OId(err,S,p,s);
            fail("Expecting: "+IllegalArgumentException.class);
        }
        catch(IllegalArgumentException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        
        // segment.
        try {
            new OId(P,err,p,s);
            fail("Expecting: "+IllegalArgumentException.class);
        }
        catch(IllegalArgumentException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        
        // page.
        try {
            new OId(P,S,err,s);
            fail("Expecting: "+IllegalArgumentException.class);
        }
        catch(IllegalArgumentException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        
        // slot.
        try {
            new OId(P,S,p,err);
            fail("Expecting: "+IllegalArgumentException.class);
        }
        catch(IllegalArgumentException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        
    }
    
    /**
     * A random non-zero object identifier component - this is 16 bits of random
     * integer but never zero (0).
     */
    public short nextComponent() {
        while( true ) {
            short comp = (short) rnd.nextInt();
            if( comp != 0 ) return comp;
        }
    }

//    /**
//     * A object identifier (may be 0L, which corresponds to a null reference). 
//     */
//    public long nextOid() {
//        return rnd.nextLong();
//    }
    
    /**
     * A non-zero object identifier in which all oid components are also
     * non-zero.
     */
    public long nextNonZeroOid() {
        short d = nextComponent();
        short c = nextComponent();
        short p = nextComponent();
        short s = nextComponent();
        long oid = OId.getOid(d, c, p, s);
        return oid;
    }

    public void test_correctRejection() {
        try {
            OId.getRelativeOid(0L, nextNonZeroOid());
            fail("Expecting: "+IllegalArgumentException.class);
        }
        catch(IllegalArgumentException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }
    }
 
}
