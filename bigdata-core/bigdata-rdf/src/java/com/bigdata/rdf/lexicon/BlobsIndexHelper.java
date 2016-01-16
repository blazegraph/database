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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.openrdf.model.BNode;
import org.openrdf.model.Value;

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
import com.bigdata.rdf.internal.INonInlineExtensionCodes;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.BlobIV;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueSerializer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.util.Bytes;
import com.bigdata.util.BytesUtil;

/**
 * Helper class for operations on the BLOBS index.
 */
public class BlobsIndexHelper {

    private static final Logger log = Logger.getLogger(BlobsIndexHelper.class);

    public static final transient int SIZEOF_HASH = Bytes.SIZEOF_INT;

    /**
     * The size of the hash collision counter.
     */
    public static final transient int SIZEOF_COUNTER = Bytes.SIZEOF_SHORT;

    /** The maximum value of the hash collision counter (unsigned short). */
    public static final transient int MAX_COUNTER = ((1 << 16) - 1);

    /** The offset at which the counter occurs in the key. */
    public static final transient int OFFSET_COUNTER = 1/* flags */+ 1/* extension */+ SIZEOF_HASH /* hashCode */;

    /** The size of a prefix key (a key without a hash collision counter). */
    public static final transient int SIZEOF_PREFIX_KEY = OFFSET_COUNTER;

    /**
     * The size of a key in the TERMS index.
     * <P>
     * Note: The key is size is ONE (1) byte for the [flags] byte, ONE (1) for
     * the extension byte (which describes what kind of non-inline IV this is),
     * FOUR (4) bytes for the hash code, plus a TWO (2) byte counter (to break
     * ties within a collision bucket).
     * <p>
     * Note: The counter size was increased when the design purpose of this
     * index was changed to handling large RDF {@link Value}s only. In practice,
     * the hash codes of the RDF {@link Value} are well distributed and
     * collisions within a hash bucket (same hash code) are rare. A ONE (1) byte
     * counter is probably all the distinctions that we could require and
     * permits up to 256 hash collisions. However, when operating in scale-out
     * the TWO (2) byte (aka short) counter provides additional confidence that
     * hash collisions will not result in a hash bucket overflow. Given that the
     * terms index will be used only with larger RDF {@link Value}s and the
     * necessity for the "extension" byte, it seems a small added cost to have
     * the TWO (2) byte counter and provides additional peace of mind. However,
     * note that scanning large collision buckets is expensive. But by allowing
     * for large collision buckets, we will pay that cost only when the hash
     * codes have an unusual distribution for some specific value.
     * <p>
     * The total key size is only 8 bytes. Since only large values are being
     * stored under the TERMS index, they will always be written as raw records
     * on the backing store. This means that we have an 8 byte key paired with
     * an 8 byte address. That allows for practical branching factors of between
     * 512 and 1024 to obtain an expected average page sizes of ~ 8k (after
     * prefix compression, etc.).
     */
    public static final transient int TERMS_INDEX_KEY_SIZE = 1 + 1
            + SIZEOF_HASH + SIZEOF_COUNTER;

    /**
     * Arbitrary threshold for the collision counter for a given hash code at
     * which we will log @ WARN. This provides notice when there are large hash
     * collision buckets which can effect performance.
     */
    public static final transient int LOG_WARN_COUNTER_THRESHOLD = 127; 
    
	/**
	 * Used to signal that the {@link Value} was not found on a read-only
	 * request.
	 */
	public static final transient int NOT_FOUND = Integer.MIN_VALUE;
	
	/**
	 * Generate the sort keys for {@link BigdataValue}s to be represented as
	 * {@link BlobIV}s. The sort key is formed from the {@link VTE} of the
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

        final ByteArrayBuffer tmp = new ByteArrayBuffer();

        final DataOutputBuffer out = new DataOutputBuffer();

        try {

            for (int i = 0; i < numTerms; i++) {

                final BigdataValue term = terms[i];

                final VTE vte = VTE.valueOf(term);

                final int hashCode = term.hashCode();

                final byte[] key = makePrefixKey(keyBuilder.reset(), vte,
                        hashCode);

                final byte[] val = valSer.serialize(term, out.reset(), tmp);

                a[i] = new KVO<BigdataValue>(key, val, term);

            }

        } finally {

            try {
                /*
                 * Note: Both the outer and inner try/catch are just to please
                 * find bugs. DataOutputStream.close() is a NOP.
                 */
                out.close();
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }

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
     * @param tmp
     *            The buffer used to format the <i>toKey</i> (optional). A new
     *            byte[] will be allocated if this is <code>null</code>, but the
     *            same byte[] can be reused for multiple invocations. The buffer
     *            MUST be dimensioned to
     *            {@link BlobsIndexHelper#SIZEOF_PREFIX_KEY}.
     * @param bucketSize
     *            The size of the collision bucket is reported as a side-effect
     *            (optional).
     * 
     * @return The collision counter for the key under which the {@link Value}
     *         was found (if pre-existing), the collision counter assigned to
     *         the {@link Value} iff the value was not found and the operation
     *         permitted writes -or- {@link Integer#MIN_VALUE} iff the
     *         {@link Value} is not in the index and the operation is read-only.
     * 
     * @throws CollisionBucketSizeException
     *             if an attempt is made to insert a {@link Value} into a
     *             collision bucket which is full.
     */
	public int resolveOrAddValue(final IIndex termsIndex,
			final boolean readOnly, final IKeyBuilder keyBuilder,
			final byte[] baseKey, final byte[] val, final byte[] tmp,
			final AtomicInteger bucketSize) {

        assert baseKey.length == BlobsIndexHelper.SIZEOF_PREFIX_KEY : "Expecting "
                + BlobsIndexHelper.SIZEOF_PREFIX_KEY
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

        // key strictly LT any successor of the hash code of this val.
        final byte[] toKey = makeToKey(fromKey, tmp);

		// fast range count. this tells us how many collisions there are.
		// this is an exact collision count since we are not deleting tuples
		// from the TERMS index.
        final long rangeCount = termsIndex.rangeCount(fromKey, toKey);
        
		if (bucketSize != null)
			bucketSize.set((int) rangeCount);
        
        if (rangeCount == 0 && readOnly) {

            // Fast path.
            return NOT_FOUND;
            
        }
        
        if (rangeCount >= MAX_COUNTER) {

            /*
             * Impose a hard limit on the #of hash collisions we will accept in
             * this utility.
             */

			throw new CollisionBucketSizeException(rangeCount);
		}

//		// Force range count into (signed) byte
//		final byte counter = (byte) rangeCount;
		
		if (rangeCount == 0) {

		    assert !readOnly;
		    
			/*
			 * This is the first time we have observed a Value which
			 * generates this hash code, so append a [short] ZERO (0) to
			 * generate the actual key and then insert the Value into the
			 * index. Since there is nothing in the index for this hash
			 * code, no collision is possible and we do not need to test the
			 * index for the value before inserting the value into the
			 * index.
			 */
			final byte[] key = makeKey(keyBuilder.reset(), baseKey,
					(int) rangeCount);

			if (termsIndex.insert(key, val) != null) {

				throw new AssertionError();
				
			}
			
			return (int) rangeCount;
			
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

            final ByteArrayBuffer tb = tuple.getValueBuffer();

//          final byte[] tmp2 = tuple.getValue();
//
//        // Note: Compares the compressed values ;-)
//        if(BytesUtil.bytesEqual(val, tmp2)) {

            // compare without materializing the tuple's value
            if (0 == BytesUtil.compareBytesWithLenAndOffset(
                    0/* aoff */, val.length/* alen */, val,//
                    0/* boff */, tb.limit()/* blen */, tb.array()/* b */
            )) {

				// Already in the index.
                final short asFoundCounter = KeyBuilder.decodeShort(tuple
                        .getKeyBuffer().array(), OFFSET_COUNTER);

				return asFoundCounter;

            }

        }

        if (readOnly) {

            // Not found.
            return NOT_FOUND;

        }

        /*
         * Hash collision.
         */

        final byte[] key = makeKey(keyBuilder.reset(), baseKey,
                (int) rangeCount);

        // Insert into the index.
        if (termsIndex.insert(key, val) != null) {

            throw new AssertionError();

		}

		if (rangeCount >= LOG_WARN_COUNTER_THRESHOLD) {

			log.warn("Collision: hashCode=" + BytesUtil.toString(key)
					+ ", collisionBucketSize=" + rangeCount);
		
		}

		return (int) rangeCount;

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
     * @param tmp
     *            The buffer used to format the <i>toKey</i> (optional). A new
     *            byte[] will be allocated if this is <code>null</code>, but the
     *            same byte[] can be reused for multiple invocations. The buffer
     *            MUST be dimensioned to
     *            {@link BlobsIndexHelper#SIZEOF_PREFIX_KEY}.
     * 
     * @return The collision counter.
     * 
     * @throws CollisionBucketSizeException
     *             if an attempt is made to insert a {@link Value} into a
     *             collision bucket which is full.
     */
    public int addBNode(final IIndex ndx, final IKeyBuilder keyBuilder,
            final byte[] baseKey, final byte[] val, final byte[] tmp) {

        /*
         * The fromKey is strictly LT any full key for the hash code of this val
         * but strictly GT any key have a hash code LT the hash code of this
         * val.
         */
        final byte[] fromKey = baseKey;

        // key strictly LT any successor of the hash code of this val.
        final byte[] toKey = makeToKey(fromKey, tmp);

        // fast range count. this tells us how many collisions there are.
        // this is an exact collision count since we are not deleting tuples
        // from the TERMS index.
        final long rangeCount = ndx.rangeCount(fromKey, toKey);

        if (rangeCount >= MAX_COUNTER) {

            /*
             * Impose a hard limit on the #of hash collisions we will accept in
             * this utility.
             */

            throw new CollisionBucketSizeException(rangeCount);
            
        }

//        // Force range count into (signed) byte
//        final byte counter = (byte) rangeCount;

        // Form a key using the collision counter (guaranteed distinct).
		final byte[] key = makeKey(keyBuilder.reset(), baseKey,
				(int) rangeCount);

        // Insert into the index.
        if (ndx.insert(key, val) != null) {

            throw new AssertionError();

        }

        if (rangeCount >= LOG_WARN_COUNTER_THRESHOLD) {

            log.warn("Collision: hashCode=" + BytesUtil.toString(key)
                    + ", collisionBucketSize=" + rangeCount);
        
        }

		return (int) rangeCount;

    }

	/**
	 * Return the value associated with the {@link BlobIV} in the TERMS index.
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
	public byte[] lookup(final IIndex ndx, final BlobIV<?> iv,
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

        final byte[] key = keyBuilder.append(baseKey).append((short) counter)
                .getKey();
        
        assert key.length == TERMS_INDEX_KEY_SIZE;
        
        return key;

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
	// Note: Only used by the unit tests.
    public byte[] makeKey(final IKeyBuilder keyBuilder, final VTE vte,
            final int hashCode, final int counter) {

		/*
		 * Note: This MUST agree with TermId#encode().
		 */
		
		keyBuilder.appendSigned(BlobIV.toFlags(vte)); // flags byte
		
		keyBuilder.appendSigned(INonInlineExtensionCodes.BlobIV); // extension byte.
		
		keyBuilder.append(hashCode); // hashCode
		
		keyBuilder.append((short) counter); // hash collision counter.

		final byte[] key = keyBuilder.getKey();
		
        assert key.length == TERMS_INDEX_KEY_SIZE;
        
        return key;

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

		/*
		 * Note: This MUST agree with TermId#encode().
		 */

	    // flags byte
	    keyBuilder.appendSigned(BlobIV.toFlags(vte));
		
	    // extension byte.
	    keyBuilder.appendSigned(INonInlineExtensionCodes.BlobIV);

	    // hashCode
	    keyBuilder.append(hashCode);
		
		final byte[] prefixKey = keyBuilder.getKey();
		
        assert prefixKey.length == SIZEOF_PREFIX_KEY;
        
        return prefixKey;
		
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
     * Generate the successor of the fromKey.
     * 
     * @param fromKey
     *            The fromKey.
     * @param tmp
     *            The buffer used to format the <i>toKey</i> (optional). A new
     *            byte[] will be allocated if this is <code>null</code>, but the
     *            same byte[] can be reused for multiple invocations. The buffer
     *            MUST be dimensioned to
     *            {@link BlobsIndexHelper#TERMS_INDEX_KEY_SIZE}.
     *            
     * @return The toKey.
     */
    byte[] makeToKey(final byte[] fromKey, final byte[] tmp) {

        assert fromKey.length == SIZEOF_PREFIX_KEY;
        
        final byte[] toKey;

        if (tmp == null) {
            // Allocate a temporary buffer.
            toKey = new byte[SIZEOF_PREFIX_KEY];
        } else if (tmp.length != SIZEOF_PREFIX_KEY) {
            // Caller's buffer is the wrong size.
            throw new IllegalArgumentException();
        } else {
            // Use the caller's buffer.
            toKey = tmp;
        }
        
        // Copy the fromKey into the temporary buffer.
        System.arraycopy(fromKey, 0/* srcPos */, toKey/* dest */, 0/* destPos */,
                SIZEOF_PREFIX_KEY/* length */);
        
        // Form the successor (side-effect on the toKey buffer).
        SuccessorUtil.successor(toKey);
        
        // Return the successor of the fromKey.
        return tmp;
        
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
