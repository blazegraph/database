/**

 Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

 Contact:
 SYSTAP, LLC DBA Blazegraph
 2501 Calvert ST NW #106
 Washington, DC 20008
 licenses@blazegraph.com

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
 * Created on Jun 6, 2011
 */
package com.bigdata.rdf.lexicon;

import java.util.Arrays;
import java.util.UUID;

import junit.framework.TestCase2;

import org.openrdf.model.vocabulary.XMLSchema;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KVO;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.io.SerializerUtil;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.AbstractIV;
import com.bigdata.rdf.internal.impl.BlobIV;
import com.bigdata.rdf.lexicon.BlobsWriteTask.BlobsWriteProcResultHandler;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.util.DumpLexicon;
import com.bigdata.util.BytesUtil;

/**
 * Test suite for low-level operations on the BLOBS index.
 * 
 * @author thompsonbry
 */
public class TestBlobsIndex extends TestCase2 {

	public TestBlobsIndex() {
	}

	public TestBlobsIndex(final String name) {
		super(name);
	}

	/**
	 * Unit test for generation of sort keys from {@link BigdataValue}s to be
	 * represented as {@link BlobIV}s.
	 */
	public void test_generateSortKeys() {
		
		final BlobsIndexHelper h = new BlobsIndexHelper();
		
		final String namespace = getName();

		final BigdataValueFactory vf = BigdataValueFactoryImpl
				.getInstance(namespace);

		/*
		 * Generate Values that we will use to read and write on the TERMS
		 * index.
		 */
		final BigdataValue[] values;
		{
		
			final BigdataURI uri1 = vf
					.createURI("http://www.bigdata.com/testTerm");

			final BigdataLiteral lit1 = vf.createLiteral("bigdata");

			final BigdataLiteral lit2 = vf.createLiteral("bigdata", "en");

			final BigdataLiteral lit3 = vf.createLiteral("bigdata",
					XMLSchema.STRING);

			final BigdataBNode bnode1 = vf.createBNode();

			final BigdataBNode bnode2 = vf.createBNode("abc");

			values = new BigdataValue[] { 
					uri1, 
					lit1,
					lit2, 
					lit3, 
					bnode1, 
					bnode2 
					};

		}

		// Generate the sort keys.
		final KVO<BigdataValue>[] a = h.generateKVOs(vf
				.getValueSerializer(), values, values.length);

		/*
		 * Verify that we can decode fully formed TermIVs based on these prefix
		 * keys.
		 */
		{
			
			final IKeyBuilder keyBuilder = h.newKeyBuilder();

			for (int i = 0; i < a.length; i++) {

				final KVO<BigdataValue> kvo = a[i];
				
				final byte[] baseKey = kvo.key;

				final int counter = i;
				
				// A fully formed key.
				final byte[] key = h.makeKey(keyBuilder.reset(), baseKey,
						counter);

				// Wrap as a TermId,
				final BlobIV<?> iv = (BlobIV<?>) IVUtility.decodeFromOffset(
						key, 0/* offset */);
				//new TermId(key);

				// Verify 1st byte decodes to the VTE of the Value.
				assertEquals(VTE.valueOf(kvo.obj), AbstractIV.getVTE(KeyBuilder
						.decodeByte(key[0])));

				// Verify the the VTE encoding is consistent in the other
				// direction as well.
				assertEquals(BlobIV.toFlags(VTE.valueOf(kvo.obj)), KeyBuilder
						.decodeByte(key[0]));

				// Verify VTE was correctly encoded.
				assertEquals(VTE.valueOf(kvo.obj), iv.getVTE());

				// Verify hash code was correctly encoded.
				assertEquals(kvo.obj.hashCode(), iv.hashCode());
				
				// Verify we can decode the String value of the TermIV.
				assertEquals(iv, BlobIV.fromString(iv.toString()));
				
			}

		}

	}
	
//	/**
//	 * Unit test for creating the TERMS index.
//	 */
//	public void test_termsIndex_create() {
//
//		final IRawStore store = new SimpleMemoryRawStore();
//		
//		try {
//			
//		    final String namespace = getName();
//		    
//			final BTree ndx = createTermsIndex(store, namespace);
//
////			final TermsIndexHelper h = new TermsIndexHelper();
////			
////			final IKeyBuilder keyBuilder = h.newKeyBuilder();
////			
////	        for (VTE vte : VTE.values()) {
////	            
////                // Each VTE has an associated NullIV (mapped to a [null]).
////                assertNull(ndx.lookup(BlobIV.mockIV(vte).encode(
////                        keyBuilder.reset()).getKey()));
////	            
////	        }
////			
////            // Should be one entry for each type of NullIV.
////            assertEquals(4L, ndx.rangeCount());
////
////            // Verify we visit each of those NullIVs.
////	        final ITupleIterator<BigdataValue> itr = ndx.rangeIterator();
////
////	        while(itr.hasNext()) {
////	            
////	            final ITuple<BigdataValue> tuple = itr.next();
////	            
////	            assertTrue(tuple.isNull());
////	            
////	            // The tuple is deserialized as a [null] reference.
////                assertNull(tuple.getObject());
////
////	        }
//	        
//		} finally {
//			
//			store.destroy();
//			
//		}
//		
//	}

    /**
     * Return the {@link IndexMetadata} for the TERMS index.
     * 
     * @param name
     *            The name of the index.
     *            
     * @return The {@link IndexMetadata}.
     */
	static IndexMetadata getTermsIndexMetadata(final String namespace) {
	    
        final String name = namespace + ".TERMS";

        final BigdataValueFactory valueFactory = BigdataValueFactoryImpl
                .getInstance(namespace);
        
        final IndexMetadata metadata = new IndexMetadata(name, UUID
                .randomUUID());

//      final int m = 1024;
//      final int q = 8000;
//      final int ratio = 32;
//        final int maxRecLen = 0;

//      metadata.setNodeKeySerializer(new FrontCodedRabaCoder(ratio));

//      final DefaultTupleSerializer tupleSer = new DefaultTupleSerializer(
//              new DefaultKeyBuilderFactory(new Properties()),//
//              new FrontCodedRabaCoder(ratio),//
//              CanonicalHuffmanRabaCoder.INSTANCE
//      );
//
//      metadata.setTupleSerializer(tupleSer);

        // enable raw record support.
        metadata.setRawRecords(true);

//        // set the maximum length of a byte[] value in a leaf.
//        metadata.setMaxRecLen(maxRecLen);

//      /*
//       * increase the branching factor since leaf size is smaller w/o large
//       * records.
//       */
//      metadata.setBranchingFactor(m);
//
//      // Note: You need to give sufficient heap for this option!
//      metadata.setWriteRetentionQueueCapacity(q);

        metadata.setTupleSerializer(new BlobsTupleSerializer(namespace,
                valueFactory));

        return metadata;

	}

    /**
     * Create a TERMS index.
     * 
     * @param namespace
     *            The namespace of the TERMS index (e.g., for "kb.lex" the fully
     *            qualified name of the index would be "kb.lex.TERMS").
     * 
     * @return The terms index.
     */
	static BTree createTermsIndex(final IRawStore store, final String namespace) {

        final IndexMetadata metadata = getTermsIndexMetadata(namespace);

        final BTree ndx = BTree.create(store, metadata);

//        /*
//         * Insert a tuple for each kind of VTE having a ZERO hash code and a
//         * ZERO counter and thus qualifying it as a NullIV. Each of these tuples
//         * is mapped to a null value in the index. This reserves the possible
//         * distinct NullIV keys so they can not be assigned to real Values.
//         * 
//         * Note: The hashCode of "" is ZERO, so an empty Literal would otherwise
//         * be assigned the same key as mockIV(VTE.LITERAL).
//         */
//
//        final IKeyBuilder keyBuilder = new TermsIndexHelper().newKeyBuilder();
//
//        final byte[][] keys = new byte[][] {
//            BlobIV.mockIV(VTE.URI).encode(keyBuilder.reset()).getKey(), //
//            BlobIV.mockIV(VTE.BNODE).encode(keyBuilder.reset()).getKey(), //
//            BlobIV.mockIV(VTE.LITERAL).encode(keyBuilder.reset()).getKey(), //
//            BlobIV.mockIV(VTE.STATEMENT).encode(keyBuilder.reset()).getKey(), //
//        };
//        final byte[][] vals = new byte[][] { null, null, null, null };
//        
//        // submit the task and wait for it to complete.
//        ndx.submit(0/* fromIndex */, keys.length/* toIndex */, keys, vals,
//                BatchInsertConstructor.RETURN_NO_VALUES, null/* aggregator */);

        return ndx;

    }
	
	/**
	 * Unit test for lookup and adding values to the TERMS index when blank
	 * nodes are NOT stored in the TERMS index.
	 */
	public void test_termsIndex_addLookupValues_with_standard_bnode_semantics() {

		doTermsIndexAddLookupTest(false/* toldBNodes */);

	}

	/**
	 * Unit test for lookup and adding values to the TERMS index when blank
	 * nodes are stored in the TERMS index (told bnodes semantics).
	 */
	public void test_termsIndex_addLookupValue_with_toldBNodesMode() {

		doTermsIndexAddLookupTest(true/* toldBNodes */);

	}

	/**
	 * Test helper exercises the basic operations on the TERMS index, including
	 * (a) scanning a collision buckets to resolve {@link BigdataValue}s from
	 * their prefix keys; (b) adding a {@link BigdataValue}s to the TERMS index;
	 * and (c) point lookups using an {@link IV} as a fully qualified key for
	 * the TERMS index.
	 * 
	 * @param toldBNodes
	 *            when <code>true</code> blank nodes will be inserted into the
	 *            TERMS index.
	 */
	private void doTermsIndexAddLookupTest(final boolean toldBNodes) {
		
		final IRawStore store = new SimpleMemoryRawStore();
		
		try {
			
		    final String namespace = getName();
		    
			final BTree ndx = createTermsIndex(store, namespace);

			final BigdataValueFactory vf = BigdataValueFactoryImpl
					.getInstance(namespace);
			
			final BlobsIndexHelper h = new BlobsIndexHelper();

			/*
			 * Generate Values that we will use to read and write on the TERMS
			 * index.
			 */
			final BigdataValue[] values;
			{
			
				final BigdataURI uri1 = vf
						.createURI("http://www.bigdata.com/testTerm");

                // Note: These three literals wind up with the same hash code.
                // The hash code of the literal is based only on its label.
				final BigdataLiteral lit1 = vf.createLiteral("bigdata");

				final BigdataLiteral lit2 = vf.createLiteral("bigdata", "en");

				final BigdataLiteral lit3 = vf.createLiteral("bigdata",
						XMLSchema.STRING);

				final BigdataBNode bnode1 = vf.createBNode();

				final BigdataBNode bnode2 = vf.createBNode("abc");

				values = new BigdataValue[] { 
						uri1, 
						lit1,
						lit2, 
						lit3, 
						bnode1, 
						bnode2 
						};

			}

			final KVO<BigdataValue>[] a = h.generateKVOs(vf
					.getValueSerializer(), values, values.length);

			final byte[][] keys = new byte[a.length][];
			final byte[][] vals = new byte[a.length][];
			for (int i = 0; i < a.length; i++) {
				keys[i] = a[i].key;
				vals[i] = a[i].val;
			}
			
			// First, verify that the Value(s) were not found in the index.
			{
				final boolean readOnly = true;
				final WriteTaskStats stats = new WriteTaskStats();

				final BlobsWriteProc.BlobsWriteProcConstructor ctor = new BlobsWriteProc.BlobsWriteProcConstructor(
						readOnly, toldBNodes);

				ndx.submit(0/* fromIndex */, values.length/* toIndex */, keys,
						vals, ctor, new BlobsWriteProcResultHandler(a,
								readOnly, stats));

				for (int i = 0; i < a.length; i++) {
					
					// IV was not assigned (read-only and does not pre-exist).
					assertNull(a[i].obj.getIV());
					
				}

				assertEquals(a.length, stats.nunknown.get());
				
			}

			// Now, verify the IVs are assigned on insert.
			{

				final boolean readOnly = false;
				final WriteTaskStats stats = new WriteTaskStats();

				final BlobsWriteProc.BlobsWriteProcConstructor ctor = new BlobsWriteProc.BlobsWriteProcConstructor(
						readOnly, toldBNodes);

				ndx.submit(0/* fromIndex */, values.length/* toIndex */, keys,
						vals, ctor, new BlobsWriteProcResultHandler(a,
								readOnly, stats));

				// Note: [nunknown] is only set on read.
				assertEquals(0, stats.nunknown.get());

				/*
				 * Verify that the IV is a fully qualified key for the TERMS
				 * index.
				 */
				final IKeyBuilder keyBuilder = h.newKeyBuilder();
				
				for (int i = 0; i < a.length; i++) {

					final BigdataValue expected = a[i].obj;
					
					final IV<?,?> iv = expected.getIV();

					// An IV was assigned to the BigdataValue.
					assertNotNull(iv);
					
					// Verify the VTE is consistent.
					assertEquals(VTE.valueOf(expected), iv.getVTE());

					// Verify the hash code is consistent.
					assertEquals(expected.hashCode(), iv.hashCode());

					// Encode the IV as a key.
					final byte[] key = iv.encode(keyBuilder.reset()).getKey();
					
					// Verify can encode/decode the IV.
					{
						
						final IV<?,?> decodedIV = IVUtility.decode(key);
						
						assertEquals(iv, decodedIV);
						
					}
					
					// Point lookup using the IV as the key.
					final byte[] val = ndx.lookup(key);

					// Verify point lookup succeeds.
					if (val == null) {
						fail("Could not resolve IV against index: expectedIV="
								+ iv + ", key=" + BytesUtil.toString(key));
					}

					// Decode the returned byte[] as a Value.
					final BigdataValue actual = vf.getValueSerializer()
							.deserialize(val);
				
					// Verify BigdataValues are equal()
					if (!expected.equals(actual)) {
					
						log.error(DumpLexicon.dumpBlobs(namespace, ndx));
						
						fail("Expected=" + expected + "(" + iv + "), actual="
								+ actual + "(" + actual.getIV() + ")");
					}
					
					if (log.isInfoEnabled())
						log.info("i=" + expected + ", iv=" + iv);

				}

			}

			// Finally, verify that the assigned IVs are discovered on lookup.
			{

				/*
				 * Setup an array of the expected IVs and clear out the old IVs
				 * on the BigdataValue objects.
				 * 
				 * Note: Since we can not clear the IV once it has been set, this
				 * replaces the BigdataValues in the array with new values having
				 * the same data.
				 */
				final IV[] expected = new IV[a.length];
				final BigdataValueFactory vf2 = BigdataValueFactoryImpl
						.getInstance(namespace + "-not-the-same");
				for (int i = 0; i < a.length; i++) {
					final BigdataValue tmp = a[i].obj;
					assertNotNull(a[i].obj.getIV()); // IV is known (from above)
					expected[i] = a[i].obj.getIV(); // make a note of it.
					final BigdataValue newVal = vf.asValue(vf2.asValue(tmp));
					// replace entry in a[].
					a[i] = new KVO<BigdataValue>(a[i].key, a[i].val, newVal);
					assertEquals(tmp, a[i].obj);// same Value.
					assertNull(a[i].obj.getIV()); // but IV is not set.
				}

				final boolean readOnly = true;
				final WriteTaskStats stats = new WriteTaskStats();

				final BlobsWriteProc.BlobsWriteProcConstructor ctor = new BlobsWriteProc.BlobsWriteProcConstructor(
						readOnly, toldBNodes);

				ndx.submit(0/* fromIndex */, values.length/* toIndex */, keys,
						vals, ctor, new BlobsWriteProcResultHandler(a,
								readOnly, stats));

				int nnotfound = 0;
				for (int i = 0; i < a.length; i++) {

				    final IV<?,?> expectedIV = expected[i];
				    
					final IV<?,?> actualIV = a[i].obj.getIV();
					
					if(expectedIV.isBNode()) {
                        if (toldBNodes) {
                            // IV is discoverable.
                            assertNotNull(actualIV);
                            assertEquals(expected[i], actualIV);
                        } else {
                            // IV is NOT discoverable (can not unify bnodes).
							if (actualIV != null)
								fail("Not expecting to unify blank node: expectedIV="
										+ expectedIV
										+ ", but actualIV="
										+ actualIV + "(should be null)");
                            nnotfound++;
                        }
                    } else {
                        // IV is discoverable.
                        assertNotNull(actualIV);
                        assertEquals(expected[i], actualIV);
                    }
					
				}
				
				assertEquals(nnotfound, stats.nunknown.get());
				
			}

		} finally {
			
			store.destroy();
			
		}
		
	}

    /**
     * Unit test with standard blank nodes semantics verifies that separate
     * writes on the TERMS index using the same BNode ID result in distinct keys
     * being assigned (blank nodes do not unify).
     */
    public void test_blank_nodes_are_distinct() {

        final boolean storeBlankNodes = false;

        final IRawStore store = new SimpleMemoryRawStore();

        try {

            final String namespace = getName();
            
            final IndexMetadata metadata = getTermsIndexMetadata(namespace);

            final BTree ndx = BTree.create(store, metadata);

            final BigdataValueFactory vf = BigdataValueFactoryImpl
                    .getInstance(namespace);

            final BlobsIndexHelper h = new BlobsIndexHelper();

            // Write on the TERMS index, obtaining IVs for those BNodes.
            final IV[] ivs1;
            {

                /*
                 * Generate Values that we will use to read and write on the
                 * TERMS index.
                 */
                final BigdataValue[] values;
                {

                    final BigdataBNode bnode1 = vf.createBNode();

                    final BigdataBNode bnode2 = vf.createBNode("abc");

                    values = new BigdataValue[] { bnode1, bnode2 };

                }

                final KVO<BigdataValue>[] a = h.generateKVOs(vf
                        .getValueSerializer(), values, values.length);

                final byte[][] keys = new byte[a.length][];
                final byte[][] vals = new byte[a.length][];
                for (int i = 0; i < a.length; i++) {
                    keys[i] = a[i].key;
                    vals[i] = a[i].val;
                }

                final boolean readOnly = false;
                final WriteTaskStats stats = new WriteTaskStats();

                final BlobsWriteProc.BlobsWriteProcConstructor ctor = new BlobsWriteProc.BlobsWriteProcConstructor(
                        readOnly, storeBlankNodes);

                ndx.submit(0/* fromIndex */, values.length/* toIndex */, keys,
                        vals, ctor, new BlobsWriteProcResultHandler(a,
                                readOnly, stats));

                // Copy out the assigned IVs.
                ivs1 = new IV[a.length];
                for (int i = 0; i < a.length; i++) {
                    final IV<?,?> iv = a[i].obj.getIV();
                    assertNotNull(iv);
                    ivs1[i] = iv;
                }

            }

            // Write on the TERMS index, obtaining new IVs for those BNodes.
            final IV[] ivs2;
            {

                /*
                 * Generate Values that we will use to read and write on the
                 * TERMS index (we need distinct instances since the IV once
                 * set can not be cleared from the BigdataValue).
                 */
                final BigdataValue[] values;
                {

                    final BigdataBNode bnode1 = vf.createBNode();

                    final BigdataBNode bnode2 = vf.createBNode("abc");

                    values = new BigdataValue[] { bnode1, bnode2 };

                }

                final KVO<BigdataValue>[] a = h.generateKVOs(vf
                        .getValueSerializer(), values, values.length);

                final byte[][] keys = new byte[a.length][];
                final byte[][] vals = new byte[a.length][];
                for (int i = 0; i < a.length; i++) {
                    keys[i] = a[i].key;
                    vals[i] = a[i].val;
                }

                final boolean readOnly = false;
                final WriteTaskStats stats = new WriteTaskStats();

                final BlobsWriteProc.BlobsWriteProcConstructor ctor = new BlobsWriteProc.BlobsWriteProcConstructor(
                        readOnly, storeBlankNodes);

                ndx.submit(0/* fromIndex */, values.length/* toIndex */, keys,
                        vals, ctor, new BlobsWriteProcResultHandler(a,
                                readOnly, stats));

                // Copy out the assigned IVs.
                ivs2 = new IV[a.length];
                for (int i = 0; i < a.length; i++) {
                    final IV<?,?> iv = a[i].obj.getIV();
                    assertNotNull(iv);
                    ivs2[i] = iv;
                }

            }

            /*
             * Verify that all assigned IVs are distinct and that all assigned
             * IVs can be used to materialize the blank nodes that we wrote onto
             * the TERMS index.
             */
            {

                final IKeyBuilder keyBuilder = h.newKeyBuilder(); 
                
                // Same #of IVs.
                assertEquals(ivs1.length, ivs2.length);

                for (int i = 0; i < ivs1.length; i++) {
                    assertNotNull(ivs1[i]);
                    assertNotNull(ivs2[i]);
                    assertNotSame(ivs1[i], ivs2[i]);
                    assertNotNull(h.lookup(ndx, (BlobIV<?>)ivs1[i], keyBuilder));
                    assertNotNull(h.lookup(ndx, (BlobIV<?>)ivs2[i], keyBuilder));
//					assertNotNull(ndx.lookup(ivs1[i]));
//					assertNotNull(ndx.lookup(ivs2[i]));
                }
                
            }

        } finally {

            store.destroy();

        }

    }

	/*
	 * FIXME These tests for hash collisions no longer work because the hash
	 * code is being computed from the original Value which makes the override
	 * of generateKeys() impossible. This makes it quite difficult to unit test
	 * the behavior which handles hash collisions since they are otherwise quite
	 * rare.
	 * 
	 * One way to do this would be to factor out the hash function such that we
	 * could override it explicitly.
	 */
    
//    /**
//     * Unit test with a no collisions.
//     */
//    public void test_noCollisions() {
//
//        doHashCollisionTest(1);
//
//    }
//
//    /**
//     * Unit test with a small number of collisions.
//     */
//    public void test_someCollisions() {
//
//        doHashCollisionTest(12);
//
//    }
//
//    /**
//     * Unit test with a the maximum number of collisions.
//     */
//    public void test_lotsOfCollisions() {
//
//		doHashCollisionTest(255);
//
//	}
//
//	/**
//	 * Unit test with a too many collisions.
//	 */
//	public void test_tooManyCollisions() {
//
//		try {
//			doHashCollisionTest(257);
//			fail("Expecting: " + CollisionBucketSizeException.class);
//		} catch (CollisionBucketSizeException ex) {
//			if (log.isInfoEnabled())
//				log.info("Ignoring expected exception: " + ex);
//		}
//
//	}
//
//	/**
//	 * Test helper attempts to insert the given number of {@link BigdataValue}s
//	 * into the terms index. If the maximum collision bucket size is reached,
//	 * then the exception is thrown back to the caller.
//	 * <p>
//	 * Note: This test needs to be done using mock data since it would take a
//	 * huge number of values to have that many collisions otherwise.
//	 * 
//	 * @param ncollisions
//	 *            The number of collisions to manufacture.
//	 *            
//	 * @throws CollisionBucketSizeException
//	 */
//	private void doHashCollisionTest(final int ncollisions) {
//		
//		final IRawStore store = new SimpleMemoryRawStore();
//		
//		try {
//			
//			final TermsIndexHelper h = new TermsIndexHelper();
//
//            final String namespace = getName();
//            
//            final IndexMetadata metadata = getTermsIndexMetadata(namespace);
//
//			final BTree ndx = BTree.create(store, metadata);
//
//			final BigdataValueFactory vf = BigdataValueFactoryImpl
//					.getInstance(namespace);
//
//			/*
//			 * Generate Values that we will use to read and write on the TERMS
//			 * index.
//			 */
//			final BigdataValue[] values;
//			{
//
//				final ArrayList<BigdataValue> tmp = new ArrayList<BigdataValue>(
//						ncollisions);
//
//				for (int i = 0; i < ncollisions; i++) {
//
//					final BigdataURI uri = vf
//							.createURI("http://www.bigdata.com/testTerm/" + i);
//
//					tmp.add(uri);
//
//				}
//
//				values = tmp.toArray(new BigdataValue[ncollisions]);
//
//			}
//
//			final int hashCode = 12;
//			final KVO<BigdataValue>[] a = mockGenerateKVOs(vf
//					.getValueSerializer(), values, values.length, hashCode);
//
//			final byte[][] keys = new byte[a.length][];
//			final byte[][] vals = new byte[a.length][];
//			for (int i = 0; i < a.length; i++) {
//				keys[i] = a[i].key;
//				vals[i] = a[i].val;
//				// Verify 1st byte decodes to the VTE of the Value.
//				assertEquals(VTE.valueOf(a[i].obj), AbstractIV
//						.getVTE(KeyBuilder.decodeByte(a[i].key[0])));
//			}
//
//			{
//
//				final boolean readOnly = false;
//				final boolean storeBlankNodes = false;
//				final WriteTaskStats stats = new WriteTaskStats();
//
//				final TermsWriteProc.TermsWriteProcConstructor ctor = new TermsWriteProc.TermsWriteProcConstructor(
//						readOnly, storeBlankNodes);
//
//				ndx.submit(0/* fromIndex */, values.length/* toIndex */, keys,
//						vals, ctor, new TermsWriteProcResultHandler(a,
//								readOnly, stats));
//
//				// Note: [nunknown] is only set on read.
//				assertEquals(0, stats.nunknown.get());
//
//				/*
//				 * Verify that the IV is a fully qualified key for the TERMS
//				 * index.
//				 */
//				final IKeyBuilder keyBuilder = h.newKeyBuilder();
//
//				for (int i = 0; i < a.length; i++) {
//
//					final BigdataValue expectedValue = a[i].obj;
//
//					final IV<?, ?> actualIV = expectedValue.getIV();
//
//					// Verify an IV was assigned.
//					assertNotNull(actualIV);
//
//					// Verify the VTE is consistent.
//					assertEquals(VTE.valueOf(expectedValue), actualIV.getVTE());
//
//					// Verify that the hashCode is consistent.
//					assertEquals(expectedValue.hashCode(), actualIV.hashCode());
//
//					// Encode the IV as a key.
//					final byte[] key = actualIV.encode(keyBuilder.reset())
//							.getKey();
//
//					// Verify can encode/decode the IV.
//					{
//
//						final IV<?, ?> decodedIV = IVUtility.decode(key);
//
//						assertEquals(actualIV, decodedIV);
//
//						assertEquals(key, decodedIV.encode(h.newKeyBuilder())
//								.getKey());
//
//					}
//					
//					// Point lookup using the IV as the key.
//					final byte[] val = ndx.lookup(key);
//
//					// Verify point lookup succeeds.
//					if (val == null) {
//						fail("Could not resolve IV against index: expectedValue="
//								+ expectedValue
//								+ ", actualIV="
//								+ actualIV
//								+ ", key=" + BytesUtil.toString(key));
//					}
//
//					// Decode the returned byte[] as a Value.
//					final BigdataValue actual = vf.getValueSerializer()
//							.deserialize(val);
//
//					// Verify BigdataValues are equal()
//					assertEquals(expectedValue, actual);
//					
//					if (log.isInfoEnabled())
//						log.info("i=" + expectedValue + ", iv=" + actualIV);
//
//				}
//
//			}
//
//		} finally {
//
//			store.destroy();
//
//		}
//
//	}

    /**
     * Create a TERMS index, put some data into it, and verify that we can use
     * the {@link BlobsTupleSerializer} to access that data, including handling
     * of the NullIV.
     */
    public void test_TermsTupleSerializer() {

        final IRawStore store = new SimpleMemoryRawStore();
        
        try {
            
            final String namespace = getName();

            final BTree ndx = createTermsIndex(store, namespace);

            final BlobsIndexHelper h = new BlobsIndexHelper();

            final BigdataValueFactory vf = BigdataValueFactoryImpl
                    .getInstance(namespace);

            /*
             * Generate Values that we will use to read and write on the TERMS
             * index.
             */
            final BigdataValue[] values;
            {

                final BigdataURI uri1 = vf
                        .createURI("http://www.bigdata.com/testTerm");

                final BigdataLiteral lit1 = vf.createLiteral("bigdata");

                final BigdataLiteral lit2 = vf.createLiteral("bigdata", "en");

                final BigdataLiteral lit3 = vf.createLiteral("bigdata",
                        XMLSchema.STRING);

                final BigdataBNode bnode1 = vf.createBNode();

                final BigdataBNode bnode2 = vf.createBNode("abc");

                values = new BigdataValue[] { uri1, lit1, lit2, lit3, bnode1,
                        bnode2 };

            }

            final KVO<BigdataValue>[] a = h.generateKVOs(vf
                    .getValueSerializer(), values, values.length);

            final byte[][] keys = new byte[a.length][];
            final byte[][] vals = new byte[a.length][];
            for (int i = 0; i < a.length; i++) {
                keys[i] = a[i].key;
                vals[i] = a[i].val;
            }

            /*
             * Write on the TERMS index, setting IVs as side-effect on
             * BigdataValues.
             */
            {

                final boolean readOnly = false;
                final boolean storeBlankNodes = true;
                final WriteTaskStats stats = new WriteTaskStats();

                final BlobsWriteProc.BlobsWriteProcConstructor ctor = new BlobsWriteProc.BlobsWriteProcConstructor(
                        readOnly, storeBlankNodes);

                ndx.submit(0/* fromIndex */, values.length/* toIndex */, keys,
                        vals, ctor, new BlobsWriteProcResultHandler(a,
                                readOnly, stats));

            }
            
            /*
             * Exercise the TermsTupleSerializer
             */
            {
                
                final BlobsTupleSerializer tupSer = (BlobsTupleSerializer) ndx
                        .getIndexMetadata().getTupleSerializer();
                
                for(BigdataValue value : values) {
                    
                    final IV<?,?> iv = value.getIV();
                    
                    assertNotNull(iv);
                    
                    // Test serializeKey.
                    final byte[] key = tupSer.serializeKey(iv);

                    final byte[] val = ndx.lookup(key);

                    final BigdataValue actualValue = vf.getValueSerializer()
                            .deserialize(val);

                    assertEquals(value, actualValue);
                    
                    /*
                     * TODO It should be possible to test more of the tupleSer
                     * directly. E.g., by running an iterator over the tuples
                     * and visiting them.
                     */
                    
                }
                
            }

        } finally {

            store.destroy();

        }

    }

//	/**
//	 * Mock variant of
//	 * {@link TermsIndexHelper#generateKVOs(BigdataValueSerializer, BigdataValue[], int)}
//	 * which uses a constant value for the assigned hash codes.
//	 * 
//	 * @param valSer
//	 *            The object used to generate the values to be written onto the
//	 *            index.
//	 * @param terms
//	 *            The terms whose sort keys will be generated.
//	 * @param numTerms
//	 *            The #of terms in that array.
//	 * @param hashCode
//	 *            The hash code to assign to each value.
//	 * 
//	 * @return An array of correlated key-value-object tuples.
//	 */
//	@SuppressWarnings("unchecked")
//	static private KVO<BigdataValue>[] mockGenerateKVOs(
//			final BigdataValueSerializer<BigdataValue> valSer,
//			final BigdataValue[] terms, final int numTerms, final int hashCode) {
//
//		if (valSer == null)
//			throw new IllegalArgumentException();
//		if (terms == null)
//			throw new IllegalArgumentException();
//		if (numTerms <= 0 || numTerms > terms.length)
//			throw new IllegalArgumentException();
//
//		final KVO<BigdataValue>[] a = new KVO[numTerms];
//
//		final TermsIndexHelper helper = new TermsIndexHelper();
//		
//		final IKeyBuilder keyBuilder = helper.newKeyBuilder();
//
//		final DataOutputBuffer out = new DataOutputBuffer();
//
//		final ByteArrayBuffer tmp = new ByteArrayBuffer();
//
//		for (int i = 0; i < numTerms; i++) {
//
//			final BigdataValue term = terms[i];
//
//			final VTE vte = VTE.valueOf(term);
//
//			final byte[] key = helper.makePrefixKey(keyBuilder.reset(), vte,
//					hashCode);
//
//			final byte[] val = valSer.serialize(term, out.reset(), tmp);
//
//			a[i] = new KVO<BigdataValue>(key, val, term);
//
//		}
//
//		return a;
//
//	}

    /**
     * Unit test for {@link BlobsWriteProc.Result} serialization.
     */
    public void test_blobsResultSerialization() {
       
        final long totalBucketSize = 7L;
        final int maxBucketSize = 3;
        final int[] counters = new int[] { 1, 0, 2, BlobsIndexHelper.NOT_FOUND,
                3 };
        
        final BlobsWriteProc.Result given = new BlobsWriteProc.Result(
                totalBucketSize, maxBucketSize, counters);
        
        assertEquals("totalBucketSize", totalBucketSize, given.totalBucketSize);
        assertEquals("maxBucketSize", maxBucketSize, given.maxBucketSize);
        assertTrue("counters[]", Arrays.equals(counters, given.counters));

        final byte[] b = SerializerUtil.serialize(given);

        final BlobsWriteProc.Result actual = (BlobsWriteProc.Result) SerializerUtil
                .deserialize(b);

        assertEquals("totalBucketSize", totalBucketSize, actual.totalBucketSize);
        assertEquals("maxBucketSize", maxBucketSize, actual.maxBucketSize);
        assertTrue("counters[]", Arrays.equals(counters, actual.counters));

    }

}
