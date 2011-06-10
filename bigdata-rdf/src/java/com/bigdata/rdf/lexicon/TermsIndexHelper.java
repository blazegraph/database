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
 * Created on Jun 6, 2011
 */
package com.bigdata.rdf.lexicon;

import org.apache.log4j.Logger;
import org.openrdf.model.BNode;
import org.openrdf.model.Value;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KVO;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.keys.SuccessorUtil;
import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.TermId;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.model.BigdataValueSerializer;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Helper class for operations on the TERMS index.
 */
public class TermsIndexHelper {

    private static final Logger log = Logger.getLogger(TermsIndexHelper.class);

	/**
	 * The size of a key in the TERMS index.
	 * <P>
	 * Note: The key is size is ONE (1) byte for the [flags] byte, FOUR (4)
	 * bytes for the hash code, plus a ONE (1) byte counter (to break ties
	 * within a collision bucket).
	 */
	public static final transient int TERMS_INDEX_KEY_SIZE = 1 + Bytes.SIZEOF_INT + 1;

	/**
	 * Generate the sort keys for {@link BigdataValue}s to be represented as
	 * {@link TermId}s. The sort key is formed from the {@link VTE} of the
	 * {@link BigdataValue} followed by the hashCode of the {@link BigdataValue}
	 * . Note that the sort key formed in this manner is only a prefix key for
	 * the TERMS index. The fully formed key also includes a counter to breaks
	 * ties when the sort key formed from {@link VTE} and hashCode results in a
	 * collision (different {@link BigdataValue}s having the same prefix key).
	 * 
	 * @param valSer
	 *            The object used to generate the values to be written onto the
	 *            index.
	 * @param terms
	 *            The terms whose sort keys will be generated.
	 * @param numTerms
	 *            The #of terms in that array.
	 * 
	 * @return An array of correlated key-value-object tuples.
	 */
	@SuppressWarnings("unchecked")
	public KVO<BigdataValue>[] generateKVOs(
			final BigdataValueSerializer<BigdataValue> valSer,
			final BigdataValue[] terms, final int numTerms) {

		if (valSer == null)
			throw new IllegalArgumentException();
		if (terms == null)
			throw new IllegalArgumentException();
		if (numTerms <= 0 || numTerms > terms.length)
			throw new IllegalArgumentException();

		final KVO<BigdataValue>[] a = new KVO[numTerms];

		final IKeyBuilder keyBuilder = newKeyBuilder();

		final DataOutputBuffer out = new DataOutputBuffer();

		final ByteArrayBuffer tmp = new ByteArrayBuffer();

		for (int i = 0; i < numTerms; i++) {

			final BigdataValue term = terms[i];

			final VTE vte = VTE.valueOf(term);

			final int hashCode = term.hashCode();
			
			final byte[] key = makePrefixKey(keyBuilder.reset(), vte, hashCode);

			final byte[] val = valSer.serialize(term, out.reset(), tmp);

			a[i] = new KVO<BigdataValue>(key, val, term);

		}

		return a;

	}

	/**
	 * Resolve an existing record in the TERMS index and insert the record if
	 * none is found.
	 * 
	 * @param termsIndex
	 *            The TERMS index.
	 * @param readOnly
	 *            <code>true</code> iff the operation is read only.
	 * @param keyBuilder
	 *            The buffer will be reset as necessary.
	 * @param baseKey
	 *            The base key for the hash code (without the counter suffix).
	 * @param val
	 *            The (serialized and compressed) RDF Value.
	 * 
	 * @return The key under which the {@link Value} was found -or-
	 *         <code>null</code> iff the {@link Value} is not in the index and
	 *         the operation is read-only.
	 * 
	 * @throws CollisionBucketSizeException
	 *             if an attempt is made to insert a {@link Value} into a
	 *             collision bucket which is full.
	 * 
	 *             TODO All we really need to return is the counter since the
	 *             client already has the baseKey. That would be less NIO and
	 *             could be an int[] return here (assuming up to an int counter,
	 *             or a short[], or a byte[]).
	 */
    public byte[] resolveOrAddValue(final IIndex termsIndex,
            final boolean readOnly, final IKeyBuilder keyBuilder,
            final byte[] baseKey, final byte[] val) {

        assert baseKey.length == TermsIndexHelper.TERMS_INDEX_KEY_SIZE - 1 : "Expecting "
                + (TermsIndexHelper.TERMS_INDEX_KEY_SIZE - 1)
                + " bytes, not "
                + baseKey.length;

		/*
		 * This is the fixed length hash code prefix. When a collision
		 * exists we can either append a counter -or- use more bits from the
		 * prefix. An extensible hash index works by progressively
		 * increasing the #of bits from the hash code which are used to
		 * create a distinction in the index. Records with identical hash
		 * values are stored in an (unordered, and possibly chained) bucket.
		 * We can approximate this by using N-bits of the hash code for the
		 * key and then increasing the #of bits in the key when there is a
		 * hash collision. Unless a hash function is used which has
		 * sufficient bits available to ensure that there are no collisions,
		 * we may be forced eventually to append a counter to impose a
		 * distinction among records which are hash identical but whose
		 * values differ.
		 * 
		 * In the case of a hash collision, we can determine the records
		 * which have already collided using the fast range count between
		 * the hash code key and the fixed length successor of that key. We
		 * can create a guaranteed distinct key by creating a BigInteger
		 * whose values is (#collisions+1) and appending it to the key. This
		 * approach will give us keys whose byte length increases slowly as
		 * the #of collisions grows (though these might not be the minimum
		 * length keys - depending on how we are encoding the BigInteger in
		 * the key.)
		 * 
		 * When we have a hash collision, we first need to scan all of the
		 * collision records and make sure that none of those records has
		 * the same value as the given record. This is done using the fixed
		 * length successor of the hash code key as the exclusive upper
		 * bound of a key range scan. Each record associated with a tuple in
		 * that key range must be compared for equality with the given
		 * record to decide whether or not the given record already exists
		 * in the index.
		 * 
		 * The fromKey is strictly LT any full key for the hash code of this
		 * val but strictly GT any key have a hash code LT the hash code of
		 * this val.
		 */
		final byte[] fromKey = baseKey;

        // key strictly LT any successor of the hash code of this val TODO More
        // efficient if we reuse the caller's fixed length buffer for this for
        // each call in a batch.
        final byte[] toKey = SuccessorUtil.successor(fromKey.clone());

		// fast range count. this tells us how many collisions there are.
		// this is an exact collision count since we are not deleting tuples
		// from the TERMS index.
        final long rangeCount = termsIndex.rangeCount(fromKey, toKey);
        
        if (rangeCount == 0 && readOnly) {

            // Fast path.
            return null; // Not found.
            
        }
        
        if (rangeCount >= 255/* unsigned byte */) {

            /*
             * Impose a hard limit on the #of hash collisions we will accept in
             * this utility.
             * 
             * TODO We do not need to have a hard limit if we use BigInteger for
             * the counter, but the performance will go through the floor if we
             * have to scan 32k entries on a hash collision!
             */

			throw new CollisionBucketSizeException(rangeCount);
		}

		// Force range count into (signed) byte
		final byte counter = (byte) rangeCount;
		
		if (rangeCount == 0) {

		    assert !readOnly;
		    
//			if(readOnly) {
//				
//				// Not found.
//				return null;
//				
//			}
			
			/*
			 * This is the first time we have observed a Value which
			 * generates this hash code, so append a [short] ZERO (0) to
			 * generate the actual key and then insert the Value into the
			 * index. Since there is nothing in the index for this hash
			 * code, no collision is possible and we do not need to test the
			 * index for the value before inserting the value into the
			 * index.
			 */
			final byte[] key = makeKey(keyBuilder.reset(), baseKey, counter);
			
			if (termsIndex.insert(key, val) != null) {

				throw new AssertionError();
				
			}
			
			return key;
			
		}

		/*
		 * Iterator over that key range
		 * 
		 * Note: We need to visit the keys in case we have a match since we will
		 * need to return that key (it will become wrapped as an IV).
		 * 
		 * Note: We need to visit the values so we can test tuples within the
		 * same collision bucket (same key up to the counter) to determine
		 * whether or not the Value is in fact the same.
		 */
		final ITupleIterator<?> itr = termsIndex
				.rangeIterator(fromKey, toKey, 0/* capacity */,
						IRangeQuery.KEYS | IRangeQuery.VALS, null/* filter */);

		while(itr.hasNext()) {
			
			final ITuple<?> tuple = itr.next();

            if (tuple.isNull()) {
                // Can not match a NullIV.
                continue;
			}
			
            // raw bytes TODO More efficient if we can compare without
            // materializing the tuple's value, or compare reusing a temporary
            // buffer either from the caller or for the iterator.
            final byte[] tmp = tuple.getValue();

			// Note: Compares the compressed values ;-)
			if(BytesUtil.bytesEqual(val, tmp)) {
				
				// Already in the index.
				final byte[] key = tuple.getKey(); return key;
				
			}
			
		}
		
		if(readOnly) {

			// Not found.
			return null;
			
		}
		
		/*
		 * Hash collision.
		 */

		final byte[] key = makeKey(keyBuilder.reset(), baseKey, counter);

		// Insert into the index.
		if (termsIndex.insert(key, val) != null) {

			throw new AssertionError();

		}

		if (rangeCount >= 127) { // arbitrary limit to log @ WARN.

			log.warn("Collision: hashCode=" + BytesUtil.toString(key)
					+ ", collisionBucketSize=" + rangeCount);
		
		}

		return key;

	}

    /**
     * Add an entry for a {@link BNode} to the TERMS index (do NOT use when told
     * blank node semantics apply).
     * <p>
     * All {@link BNode}s entered by this method are distinct regardless of
     * their {@link BNode#getID()}. Since blank nodes can not be unified with
     * the TERMS index (unless we are using told blank node semantics) we simply
     * add another entry for the caller's {@link BNode} and return the key for
     * that entry which will be wrapped as an {@link IV}. That entry will be
     * made distinct from all other entries for the same {@link VTE} and
     * hashCode by appending the current collision counter (which is just the
     * range count).
     * 
     * @param termsIndex
     *            The TERMS index.
     * @param keyBuilder
     *            The buffer will be reset as necessary.
     * @param baseKey
     *            The base key for the hash code (without the counter suffix).
     * @param val
     *            The (serialized and compressed) RDF {@link BNode}.
     * 
     * @return The key for the distinct bnode.
     * 
     * @throws CollisionBucketSizeException
     *             if an attempt is made to insert a {@link Value} into a
     *             collision bucket which is full.
     * 
     *             TODO All we really need to return is the counter since the
     *             client already has the baseKey. That would be less NIO and
     *             could be an int[] return here (assuming up to an int counter,
     *             or a short[], or a byte[]).
     */
    public byte[] addBNode(final IIndex ndx, final IKeyBuilder keyBuilder,
            final byte[] baseKey, final byte[] val) {

        /*
         * The fromKey is strictly LT any full key for the hash code of this val
         * but strictly GT any key have a hash code LT the hash code of this
         * val.
         */
        final byte[] fromKey = baseKey;

        // key strictly LT any successor of the hash code of this val.
        final byte[] toKey = SuccessorUtil.successor(fromKey.clone());

        // fast range count. this tells us how many collisions there are.
        // this is an exact collision count since we are not deleting tuples
        // from the TERMS index.
        final long rangeCount = ndx.rangeCount(fromKey, toKey);

        if (rangeCount >= 255/* unsigned byte */) {

            /*
             * Impose a hard limit on the #of hash collisions we will accept in
             * this utility.
             * 
             * TODO We do not need to have a hard limit if we use BigInteger for
             * the counter, but the performance will go through the floor if we
             * have to scan 32k entries on a hash collision!
             */

            throw new CollisionBucketSizeException(rangeCount);
        }

        // Force range count into (signed) byte
        final byte counter = (byte) rangeCount;

        // Form a key using the collision counter (guaranteed distinct).
        final byte[] key = makeKey(keyBuilder.reset(), baseKey, counter);

        // Insert into the index.
        if (ndx.insert(key, val) != null) {

            throw new AssertionError();

        }

        if (rangeCount >= 127) { // arbitrary limit to log @ WARN.

            log.warn("Collision: hashCode=" + BytesUtil.toString(key)
                    + ", collisionBucketSize=" + rangeCount);
        
        }

        return key;

    }

	/**
	 * Return the value associated with the {@link TermId} in the TERMS index.
	 * <p>
	 * Note: The returned <code>byte[]</code> may be decoded using the
	 * {@link BigdataValueSerializer} associated with the
	 * {@link BigdataValueFactory} for the namespace of the owning
	 * {@link AbstractTripleStore}.
	 * 
	 * @param ndx
	 *            The index.
	 * @param iv
	 *            The {@link IV}.
	 * @param keyBuilder
	 *            An object used to format the {@link IV} as a key for the
	 *            index.
	 * 
	 * @return The byte[] value -or- <code>null</code> if there is no entry for
	 *         that {@link IV} in the index.
	 */
	public byte[] lookup(final IIndex ndx, final TermId iv,
			final IKeyBuilder keyBuilder) {

		final byte[] key = iv.encode(keyBuilder.reset()).getKey();

		return ndx.lookup(key);

	}

	/**
	 * Create a fully formed key for the TERMS index from a baseKey and a hash
	 * collision counter.
	 * 
	 * @param keyBuilder
	 *            The caller is responsible for resetting the buffer as
	 *            required.
	 * @param baseKey
	 *            The base key (including the flags byte and the hashCode).
	 * @param counter
	 *            The counter value.
	 * 
	 * @return The fully formed key.
	 */
	public byte[] makeKey(final IKeyBuilder keyBuilder, final byte[] baseKey,
			final int counter) {

		return keyBuilder.append(baseKey).appendSigned((byte) counter).getKey();

	}
	
	/**
	 * Create a fully formed key for the TERMS index from the {@link VTE}, the
	 * hashCode of the {@link BigdataValue}, and the hash collision counter.
	 * 
	 * @param keyBuilder
	 *            The caller is responsible for resetting the buffer as
	 *            required.
	 * @param vte
	 *            The {@link VTE}.
	 * @param hashCode
	 *            The hash code of the {@link BigdataValue}.
	 * @param counter
	 *            The hash collision counter.
	 *            
	 * @return The fully formed key.
	 */
	public byte[] makeKey(final IKeyBuilder keyBuilder, final VTE vte,
			final int hashCode, final int counter) {

		keyBuilder.append(TermId.toFlags(vte)); // flags byte
		
		keyBuilder.append(hashCode); // hashCode
		
		keyBuilder.appendSigned((byte) counter); // hash collision counter.

		return keyBuilder.getKey();
		
	}

	/**
	 * Create a prefix key for the TERMS index from the {@link VTE} and hashCode
	 * of the {@link BigdataValue}.
	 * 
	 * @param keyBuilder
	 *            The caller is responsible for resetting the buffer as
	 *            required.
	 * @param vte
	 *            The {@link VTE}.
	 * @param hashCode
	 *            The hash code of the {@link BigdataValue}.
	 * 
	 * @return The prefix key.
	 */
	public byte[] makePrefixKey(final IKeyBuilder keyBuilder, final VTE vte,
			final int hashCode) {

		keyBuilder.append(TermId.toFlags(vte)); // flags byte
		
		keyBuilder.append(hashCode); // hashCode
		
		return keyBuilder.getKey();
		
	}

    /**
     * Create a prefix key for the TERMS index from the {@link BigdataValue}.
     * 
     * @param keyBuilder
     *            The caller is responsible for resetting the buffer as
     *            required.
     * @param value
     *            The {@link BigdataValue}
     * 
     * @return The prefix key.
     */
    public byte[] makePrefixKey(final IKeyBuilder keyBuilder,
            final BigdataValue value) {

        final VTE vte = VTE.valueOf(value);

        return makePrefixKey(keyBuilder, vte, value.hashCode());

    }

    /**
     * Return a new {@link IKeyBuilder} suitable for formatting keys for the
     * TERMS index.
     * 
     * @return The {@link IKeyBuilder}.
     */
    public IKeyBuilder newKeyBuilder() {

        return new KeyBuilder(TERMS_INDEX_KEY_SIZE);

    }

    public StringBuilder dump(final String namespace, final IIndex ndx) {

        final StringBuilder sb = new StringBuilder(100 * Bytes.kilobyte32);

        final BigdataValueFactory vf = BigdataValueFactoryImpl
                .getInstance(namespace);

        final BigdataValueSerializer<BigdataValue> valSer = vf
                .getValueSerializer();

        final StringBuilder tmp = new StringBuilder();

        final ITupleIterator<TermId<?>> itr = ndx.rangeIterator();

        while (itr.hasNext()) {

            final ITuple<TermId<?>> tuple = itr.next();

            final TermId iv = new TermId(tuple.getKey());

            final BigdataValue value;

            if (tuple.isNull()) {

                value = null;

            } else {

                value = valSer.deserialize(tuple.getValueStream(), tmp);

            }

            sb.append(iv.toString() + " => " + value);

            sb.append("\n");

        }

        return sb;

    }

    /**
     * Exception thrown if the maximum size of the collision bucket would be
     * exceeded for some {@link BigdataValue}.
     * 
     * @author thompsonbry
     */
    public static class CollisionBucketSizeException extends RuntimeException {

        /**
		 * 
		 */
        private static final long serialVersionUID = 1L;

        public CollisionBucketSizeException(final long rangeCount) {
            super("ncoll=" + rangeCount);
        }

    }

}
