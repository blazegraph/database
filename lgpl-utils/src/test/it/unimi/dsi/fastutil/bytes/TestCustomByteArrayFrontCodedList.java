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
 * Created on Oct 1, 2008
 */

package it.unimi.dsi.fastutil.bytes;

import it.unimi.dsi.fastutil.objects.ObjectListIterator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import junit.framework.TestCase;

/**
 * Unit tests for {@link CustomByteArrayFrontCodedList}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestCustomByteArrayFrontCodedList extends TestCase {

    /**
     * 
     */
    public TestCustomByteArrayFrontCodedList() {
    }

    /**
     * @param arg0
     */
    public TestCustomByteArrayFrontCodedList(String arg0) {
        super(arg0);
    }

    public void test() {
        
        _test(100000);
        
    }
    
    /*
     * test harness from here to the end.
     */
    
    private static long seed = System.currentTimeMillis(); 
    private static java.util.Random r = new java.util.Random( seed );

    private static byte genKey() {
        return (byte)(r.nextInt());
    }

    private static void fatal( String msg ) {
//        System.out.println( msg );
//        System.exit( 1 );
        fail(msg);
    }

    private static void ensure( boolean cond, String msg ) {
        if ( cond ) return;
        fatal( msg );
    }

    private static boolean contentEquals( java.util.List x, java.util.List y ) {
        if ( x.size() != y.size() ) return false;
        for( int i = 0; i < x.size(); i++ ) if ( ! java.util.Arrays.equals( (byte[])x.get( i ), (byte[])y.get( i ) ) ) return false;
        return true;
    }

    private int l[];
    private byte[][] a; 


    private void _test( final int n) {
        int c;

        l = new int[ n ];
        a = new byte[n][];

        for( int i = 0; i < n; i++ ) l[i] = (int)(Math.abs(r.nextGaussian())*32);
        for( int i = 0; i < n; i++ ) a[i] = new byte[l[i]];
        for( int i = 0; i < n; i++ ) for( int j = 0; j < l[i]; j++ ) a[i][j] = genKey();

        CustomByteArrayFrontCodedList m = new CustomByteArrayFrontCodedList( it.unimi.dsi.fastutil.objects.ObjectIterators.wrap( a ), r.nextInt( 4 ) + 1 );
        it.unimi.dsi.fastutil.objects.ObjectArrayList t = new it.unimi.dsi.fastutil.objects.ObjectArrayList( a );

        //System.out.println(m);
        //for( i = 0; i < t.size(); i++ ) System.out.println(ARRAY_LIST.wrap((byte[])t.get(i)));

        /* Now we check that m actually holds that data. */
          
        ensure( contentEquals( m, t ), "Error (" + seed + "): m does not equal t at creation" );

        /* Now we check cloning. */

        ensure( contentEquals( m, (java.util.List)m.clone() ), "Error (" + seed + "): m does not equal m.clone()" );

        /* Now we play with iterators. */

        {
            ObjectListIterator i;
            java.util.ListIterator j;
            Object J;
            i = m.listIterator(); 
            j = t.listIterator(); 

            for( int k = 0; k < 2*n; k++ ) {
                ensure( i.hasNext() == j.hasNext(), "Error (" + seed + "): divergence in hasNext()" );
                ensure( i.hasPrevious() == j.hasPrevious(), "Error (" + seed + "): divergence in hasPrevious()" );

                if ( r.nextFloat() < .8 && i.hasNext() ) {
                    ensure( java.util.Arrays.equals( (byte[])i.next(), (byte[])j.next() ), "Error (" + seed + "): divergence in next()" );

                }
                else if ( r.nextFloat() < .2 && i.hasPrevious() ) {
                    ensure( java.util.Arrays.equals( (byte[])i.previous(), (byte[])j.previous() ), "Error (" + seed + "): divergence in previous()" );
                }

                ensure( i.nextIndex() == j.nextIndex(), "Error (" + seed + "): divergence in nextIndex()" );
                ensure( i.previousIndex() == j.previousIndex(), "Error (" + seed + "): divergence in previousIndex()" );

            }

        }

        {
            Object previous = null;
            Object I, J;
            int from = r.nextInt( m.size() +1 );
            ObjectListIterator i;
            java.util.ListIterator j;
            i = m.listIterator( from ); 
            j = t.listIterator( from ); 

            for( int k = 0; k < 2*n; k++ ) {
                ensure( i.hasNext() == j.hasNext(), "Error (" + seed + "): divergence in hasNext() (iterator with starting point " + from + ")" );
                ensure( i.hasPrevious() == j.hasPrevious() , "Error (" + seed + "): divergence in hasPrevious() (iterator with starting point " + from + ")" );

                if ( r.nextFloat() < .8 && i.hasNext() ) {
                    ensure( java.util.Arrays.equals( (byte[])i.next(), (byte[])j.next() ), "Error (" + seed + "): divergence in next() (iterator with starting point " + from + ")" );
                    //System.err.println("Done next " + I + " " + J + "  " + badPrevious);

                }
                else if ( r.nextFloat() < .2 && i.hasPrevious() ) {
                    ensure( java.util.Arrays.equals( (byte[])i.previous(), (byte[])j.previous() ), "Error (" + seed + "): divergence in previous() (iterator with starting point " + from + ")" );

                }
            }

        }
        
        /*
         * Standard serialization.
         */
        try {
            java.io.ByteArrayOutputStream os = new ByteArrayOutputStream();
            java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(os);

            oos.writeObject(m);
            oos.close();

            ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
            java.io.ObjectInputStream ois = new java.io.ObjectInputStream(is);

            CustomByteArrayFrontCodedList m2 = (CustomByteArrayFrontCodedList) ois.readObject();
            ois.close();
            contentEquals(m, m2);
        }
        catch(Exception e) {
            throw new AssertionError(e);
        }

        /*
         * Direct byte[] (de-)serialization.
         */
        try {
            CustomByteArrayFrontCodedList m2 = new CustomByteArrayFrontCodedList(
                    m.size(),m.ratio(),m.getBackingBuffer().toArray());
            contentEquals(m, m2);
        }
        catch(Exception e) {
            throw new AssertionError(e);
        }

        ensure( contentEquals( m, t ), "Error (" + seed + "): m does not equal t after save/read" );

        /*
         * Direct ByteBuffer (de-)serialization.
         */
        try {
            CustomByteArrayFrontCodedList m2 = new CustomByteArrayFrontCodedList(
                    m.size(),m.ratio(),ByteBuffer.wrap(m.getBackingBuffer().toArray()));
            contentEquals(m, m2);
        }
        catch(Exception e) {
            throw new AssertionError(e);
        }

        ensure( contentEquals( m, t ), "Error (" + seed + "): m does not equal t after save/read" );

        System.out.println("Test OK");
        return;
    }

//    public static void main( String args[] ) {
//        int n  = Integer.parseInt(args[1]);
//        if ( args.length > 2 ) r = new java.util.Random( seed = Long.parseLong( args[ 2 ] ) );
//          
//        try {
//            if ("speedTest".equals(args[0]) || "speedComp".equals(args[0])) speedTest( n, "speedComp".equals(args[0]) );
//            else if ( "test".equals( args[0] ) ) test(n);
//        } catch( Throwable e ) {
//            e.printStackTrace( System.err );
//            System.err.println( "seed: " + seed );
//        }
//    }

}
