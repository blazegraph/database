package com.bigdata.htree;

import com.bigdata.btree.ITuple;

/**
 * Extended interface to support hash buckets.
 * 
 * @author thompsonbry@users.sourceforge.net
 * 
 * @param <E>
 * 
 * @todo The reason for having this on ITuple is to make it practical for the
 *       hash code to be defined in terms of application specific data types
 *       rather than the unsigned byte[] key (but the latter could of course be
 *       decoded by the hash function before computing the hash of the
 *       application data type, except for things like Unicode keys).
 *       <p>
 *       This should probably be lifted onto {@link ITuple} and
 *       {@link #getKeyHash()} should be declared to throw an
 *       {@link UnsupportedOperationException} if the hash code of the key is
 *       not being stored.
 */
public interface IHashTuple<E> extends ITuple<E> {

//	int getHashBitLength();
	
	/**
	 * The int32 hash value of the key.
	 */
	int getKeyHash();
	
}
