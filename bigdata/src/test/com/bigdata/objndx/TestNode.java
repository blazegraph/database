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
 * Created on Nov 4, 2006
 */

package com.bigdata.objndx;

import com.bigdata.journal.SlotMath;

/**
 * @todo write tests.
 *
 * FIXME write correct rejection tests for the various constructor forms.
 * 
 * FIXME write post-condition tests for the various constructor forms.
 * 
 * FIXME write test for key comparison, insertion sort, and binary search for
 * nodes and leaves with varying numbers of keys defined.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestNode extends AbstractObjectIndexTestCase {

    /**
     * 
     */
    public TestNode() {
    }

    /**
     * @param arg0
     */
    public TestNode(String arg0) {
        super(arg0);
    }

    /**
     * Test verifies that {@link Node#findChildren(Integer)} can find any
     * key that is present on a node. 
     */
    public void test_find01() {        
        ObjectIndex ndx = null;
        SlotMath slotMath = new SlotMath(128);
        int pageSize = 16;
        NodeSerializer nodeSer = new NodeSerializer(slotMath,pageSize);
        Node node = getRandomNodeOrLeaf(ndx, nodeSer);
        node.dump(System.err, 0);
        int first = node._first;
        if( first == pageSize-1 ) {
            return; // no keys.
        }
        for(int i=first; i<pageSize; i++ ) {
            assertEquals(i,node.findChildren(node._keys[i]));
        }
    }
    
    /**
     * Test generates a set of valid keys spaced by two units. Probes are made
     * for each key, for the value one unit less than each key, and for the
     * value one unit greater than each key. The test verifies that
     * {@link Node#findChildren(Integer)} reports the correct index position for
     * the probe in each case.
     */
    public void test_find02() {        
        
        final int pageSize = 4;
        
//        ObjectIndex ndx = null;
//        
//        SlotMath slotMath = new SlotMath(128);
        
//        NodeSerializer nodeSer = new NodeSerializer(slotMath,pageSize);
        
//        Node node = getRandomNodeOrLeaf(ndx, nodeSer);
        
        Node node = getNodeWithKeysSpacedByTwo(pageSize,pageSize/2);
        
        //node.dump(System.err, 0);
        
        final int first = node._first;
        
        for( int i=first; i<pageSize; i++ ) {
            
            final Integer[] _keys = node._keys;
            
            final int key = _keys[i];
            assert key > Node.NEGINF_KEY;
            assert key < Node.POSINF_KEY;

            /*
             * probe at index position.
             */
            {
            
                final int index = node.findChildren(key);

                assertEquals( "probe key="+key, i, index );
                
            }
            
            /*
             * Probe at value one unit less than the key. The result will be the
             * same key index unless was off by one, in which case the result is
             * an exact match on the prior key value.
             */
            if( i>first ) {
                
                // the prior key value in the array.
                final int priorKey = _keys[ i - 1 ];
                assert priorKey > Node.NEGINF_KEY;
                assert priorKey < Node.POSINF_KEY;
                
                // key numerically one less than the current key.
                final int keym1 = key - 1;

                // find the key numerically one less than the current key.
                final int index = node.findChildren(keym1);
                
                if( keym1 == priorKey ) {

                    // exact match on the priorKey.
                    assertEquals("probe key="+keym1, i-1, index);
                    
                } else {
                    
                    /*
                     * otherwise greater than the prior key and hence matching
                     * on the same index.
                     */
                    assertEquals("probe key="+keym1, i, index);
                    
                }

            }
            
            /*
             * Probe at value one unit greater than the key. The result will be
             * the next key index since find always returns the first index
             * whose key is greater than or equal to the probe.
             */
            if( i<pageSize ) {
                
                // the next key value in the array.
//                final int nextKey = (i==pageSize-1?Node.POSINF_KEY:_keys[ i + 1 ]);
//                assert nextKey > Node.NEGINF_KEY;
//                assert nextKey < Node.POSINF_KEY;
                
                // key numerically one more than the current key.
                final int keyp1 = key + 1;

                // find the key numerically one more than the current key.
                final int index = node.findChildren(keyp1);

                System.err.println("probe key=" + keyp1 + ", key[" + index
                        + "]=" + _keys[index]);

                if( keyp1 > _keys[pageSize-1] ) {
                    /*
                     * Note: The result when probing greater than the last key
                     * value is the last key position.
                     * 
                     * @todo This edge case is doubtless treated through logic
                     * everywhere that findChildren is called by {@link Node}.
                     */
                    assertEquals("probe key="+keyp1, i, index);
                } else {
                    /*
                     * Otherwise the result is the next key position.
                     */
                    assertEquals("probe key="+keyp1, i + 1, index);
                }

//                if( keyp1 == nextKey ) {
//
//                    // exact match on the nextKey.
//                    assertEquals("probe key="+keyp1, i + 1, index);
//                    
//                } else {
//                    
//                    /*
//                     * otherwise greater than this key yet less than the next
//                     * key and hence matching on the same index as this key.
//                     */
//                    assertEquals( "probe key="+keyp1, i, index );
//                    
//                }

            }
        
        }
        
    }

    /**
     * Create a node whose keys are two units apart.
     * 
     * @param pageSize
     *            The #of keys that the node can hold.
     * @param first
     *            The index of the first valid key.
     *            
     * @return The node.
     */
    public Node getNodeWithKeysSpacedByTwo(int pageSize,int first) {

        assert first<pageSize-1; // at least one used key.
        
        int[] keys = new int[pageSize];
        long[] children = new long[pageSize];
        
        int key = 1;
        for( int i=first; i<pageSize; i++ ) {
            
            keys[i] = key;
            
            key += 2;
            
        }
        
        Node node = new Node(null,0L,pageSize,first,keys,children);

        node.dump(System.err, 0);
        
        return node;
    }
    
    /**
     * Test verifies that {@link Node#findChildren(Integer)} returns the first
     * node entry having a key greater than or equal to a random key.
     */
    public void test_find03() {        
        ObjectIndex ndx = null;
        SlotMath slotMath = new SlotMath(128);
        int pageSize = 16;
        NodeSerializer nodeSer = new NodeSerializer(slotMath,pageSize);
        Node node = getRandomNodeOrLeaf(ndx, nodeSer);
        //node.dump(System.err, 0);
        int first = node._first;
        if( first == pageSize-1 ) {
            return; // no keys.
        }
        int limit = 1000;
        for( int trial=0; trial<limit; trial++) {
            // random key.
            int key = r.nextInt(Node.POSINF_KEY-1)+1;
            assert key > Node.NEGINF_KEY;
            assert key < Node.POSINF_KEY;
            final int index = node.findChildren(key);
            assert index >= first;
            assert index < pageSize;
            Integer[] _keys = node._keys;
            // verify key at that index is GTE the given key.
            if( ! ( _keys[index] >= key ) ) {
                fail("findChildren: key="+key+", but key["+index+"]="+_keys[index]);
            }
            if( index > first ) {
                // verify key at prior index is LT the given key.
                assert _keys[index-1] < key;
            }
            if( index < pageSize ) {
                // verify key at next index is GT the given key.
                assert _keys[ index+1 ] < key;
            }
        }
    }

    /**
     * @todo test find with neg inf (return first) and pos inf (returns ?).
     */
    public void test_find_neginf() {
        throw new UnsupportedOperationException();
    }

    public void test_find_posinf() {
        throw new UnsupportedOperationException();
    }
    
}
