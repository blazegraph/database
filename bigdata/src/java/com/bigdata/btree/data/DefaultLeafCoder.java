/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Aug 28, 2009
 */

package com.bigdata.btree.data;

import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.log4j.Logger;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.btree.raba.codec.ICodedRaba;
import com.bigdata.btree.raba.codec.IRabaCoder;
import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.IRawStore;

/**
 * Default implementation for immutable {@link ILeafData} records.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DefaultLeafCoder implements IAbstractNodeDataCoder<ILeafData>,
		Externalizable {

    /**
     * 
     */
    private static final long serialVersionUID = -2225107318522852096L;

    protected static final Logger log = Logger
            .getLogger(DefaultLeafCoder.class);

    protected static final byte VERSION0 = 0x00;
    
    private IRabaCoder keysCoder;
    private IRabaCoder valsCoder;

    public void readExternal(final ObjectInput in) throws IOException,
            ClassNotFoundException {

        final byte version = in.readByte();
        switch(version) {
        case VERSION0:
            break;
        default:
            throw new IOException();
        }

        keysCoder = (IRabaCoder) in.readObject();
        
        valsCoder = (IRabaCoder) in.readObject();
        
    }

    public void writeExternal(final ObjectOutput out) throws IOException {

        out.write(VERSION0);
        
        out.writeObject(keysCoder);
        
        out.writeObject(valsCoder);
        
    }

    /** Yes. */
    final public boolean isLeafDataCoder() {
        
        return true;
        
    }

    /** No. */
    public boolean isNodeDataCoder() {

        return false;
        
    }

    public String toString() {

        return super.toString() + "{keysCoder=" + keysCoder + ", valsCoder="
                + valsCoder + "}";

    }

    /**
     * De-serialization ctor.
     */
    public DefaultLeafCoder() {
        
    }
    
    /**
     * 
     * @param keysCoder
     *            The {@link IRabaCoder} for the leaf's keys.
     * @param valsCoder
     *            The {@link IRabaCoder} for the leaf's values.
     */
    public DefaultLeafCoder(final IRabaCoder keysCoder,
            final IRabaCoder valsCoder) {

        if (keysCoder == null)
            throw new IllegalArgumentException();

        if (valsCoder == null)
            throw new IllegalArgumentException();

        this.keysCoder = keysCoder;

        this.valsCoder = valsCoder;

    }

    public ILeafData decode(final AbstractFixedByteArrayBuffer data) {

        return new ReadOnlyLeafData(data, keysCoder, valsCoder);

    }

    public ILeafData encodeLive(final ILeafData leaf, final DataOutputBuffer buf) {
        
        if (leaf == null)
            throw new IllegalArgumentException();

        if (buf == null)
            throw new IllegalArgumentException();

        // cache some fields.
        final int nkeys = leaf.getKeyCount();

        // The byte offset of the start of the coded record into the buffer.
        final int O_origin = buf.pos();
        
        // Flag record as leaf or linked-leaf (vs node).
        final boolean doubleLinked = leaf.isDoubleLinked();
        buf.putByte((byte) (doubleLinked ? AbstractReadOnlyNodeData.LINKED_LEAF
                : AbstractReadOnlyNodeData.LEAF));

        if(doubleLinked) {

            /*
             * Skip over priorAddr/nextAddr fields (java will have zeroed the
             * entire buffer when we allocated it). These fields need to be
             * filled in on the record after it has been serialized (and
             * potentially after it has been compressed) so we know its space on
             * disk requirements.
             */

            buf.skip(AbstractReadOnlyNodeData.SIZEOF_ADDR * 2);
            
        }

        buf.putShort(AbstractReadOnlyNodeData.VERSION0);
        
        short flags = 0;
        final boolean hasDeleteMarkers = leaf.hasDeleteMarkers();
        final boolean hasVersionTimestamps = leaf.hasVersionTimestamps();
        final boolean hasRawRecords = leaf.hasRawRecords();
//        final boolean hasHashKeys = leaf instanceof IBucketData; // @todo add hasHashKeys() method?
        if (hasDeleteMarkers) {
            flags |= AbstractReadOnlyNodeData.FLAG_DELETE_MARKERS;
        }
        if (hasVersionTimestamps) {
            flags |= AbstractReadOnlyNodeData.FLAG_VERSION_TIMESTAMPS;
        }
        if (hasRawRecords) {
            flags |= AbstractReadOnlyNodeData.FLAG_RAW_RECORDS;
        }
//        if(hasHashKeys) {
//        	flags |= AbstractReadOnlyNodeData.FLAG_HASH_KEYS;
//        }

        buf.putShort(flags);
        
        buf.putInt(nkeys); // pack?

        final int O_keysSize = buf.pos();
        
        // skip past the keysSize and valuesSize fields.
        buf.skip(AbstractReadOnlyNodeData.SIZEOF_KEYS_SIZE * 2);
//        buf.putInt(encodedKeys.length); // keysSize
//        buf.putInt(encodedValues.length); // valuesSize

        // encode the keys into the buffer
        final ICodedRaba encodedKeys = keysCoder
                .encodeLive(leaf.getKeys(), buf);

        // encode the values into the buffer.
        final ICodedRaba encodedValues = valsCoder.encodeLive(leaf.getValues(),
                buf);

        /*
         * Patch the buffer to indicate the byte length of the encoded keys and
         * the encoded values.
         */
        buf.putInt(O_keysSize, encodedKeys.data().len());
        buf.putInt(O_keysSize + AbstractReadOnlyNodeData.SIZEOF_KEYS_SIZE,
                encodedValues.data().len());
        
        // delete markers (bit coded).
//        final int O_deleteMarkers;
        if (hasDeleteMarkers) {

//            O_deleteMarkers = buf.pos();

            for (int i = 0; i < nkeys;) {

                byte bits = 0;
                
                for (int j = 0; j < 8 && i < nkeys; j++, i++) {

                    if(leaf.getDeleteMarker(i)) {

                        // Note: bit order is per BitInputStream & BytesUtil!
                        bits |= 1 << (7 - j);
                            
                    }
                    
                }

                buf.putByte(bits);

            }

//        } else {
//        
//            O_deleteMarkers = -1;
            
        }

        // The byte offset to minVersionTimestamp.
//        final int O_versionTimestamps;
        if (hasVersionTimestamps) {

            /*
             * The (min,max) are written out as full length long values. The per
             * tuple revision timestamps are written out using the minimum #of
             * bits required to code the data.
             * 
             * Note: If min==max then ZERO bits are used per timestamp!
             */

            final long min = leaf.getMinimumVersionTimestamp();

            final long max = leaf.getMaximumVersionTimestamp();
            
//            final long delta = max - min;
//            assert delta >= 0;

            // will be in [1:64]
            final byte versionTimestampBits = (byte) (Fast
                    .mostSignificantBit(max - min) + 1);

            // one byte.
            buf.putByte((byte) versionTimestampBits);

            // offset of minVersionTimestamp.
//            O_versionTimestamps = buf.pos();

            // int64
            buf.putLong(min);

            // int64
            buf.putLong(max);

            /*
             * FIXME pre-extend the buffer and use slice ctor for obs for less
             * allocation and copying.
             * 
             * FIXME Use pluggable coding for version timestamps.
             */
            if (versionTimestampBits > 0) {
                /*
                 * Note: We only write the deltas if there is more than one
                 * distinct timestamp value (min!=max). When min==max, the
                 * deltas are coded in zero bits, so this would be a NOP anyway.
                 */
                final int byteLength = BytesUtil.bitFlagByteLength(nkeys
                        * versionTimestampBits/* nbits */);
                final byte[] a = new byte[byteLength];
                final OutputBitStream obs = new OutputBitStream(a);
                try {

                    // array of [versionTimestampBits] fields.
                    for (int i = 0; i < nkeys; i++) {

                        final long deltat = leaf.getVersionTimestamp(i) - min;
                        assert deltat >= 0;

                        obs.writeLong(deltat, versionTimestampBits);

                    }

                    obs.flush();

                    // copy onto the buffer.
                    buf.put(a);

                } catch (IOException e) {
                    throw new RuntimeException(e);
                    // Note: close is not necessary if flushed and backed by
                    // byte[].
                    // } finally {
                    // try {
                    // obs.close();
                    // } catch (IOException e) {
                    // log.error(e);
                    // }
                }
            }

//        } else {
//
//            O_versionTimestamps = -1;

        }

        // raw records (bit coded).
//      final int O_rawRecords
      if (hasRawRecords) {

//          O_rawRecords = buf.pos();

          for (int i = 0; i < nkeys;) {

              byte bits = 0;
              
              for (int j = 0; j < 8 && i < nkeys; j++, i++) {

					if (leaf.getRawRecord(i) != IRawStore.NULL) {

                      // Note: bit order is per BitInputStream & BytesUtil!
                      bits |= 1 << (7 - j);
                          
                  }
                  
              }

              buf.putByte(bits);

          }

//      } else {
//      
//          O_deleteMarkers = -1;
          
      }
      
  // hash codes of the keys (MSB prefix plus LSB coded).
//      final int O_hashKeys;
//      if (hasHashKeys) {
//
//    	  // The bit length of the hash values.
//    	  final int hashBitLength = 32;//((IBucketData)leaf).getHashBitLength();
//
//    	  // The bit length of the shared MSB prefix.
//    	  final int lengthMSB = ((IBucketData)leaf).getLengthMSB();
//
//    	  // The bit length of the LSB which differ for each hash value.
//    	  final int lengthLSB = hashBitLength - lengthMSB;
//    	  
////    	  buf.putShort((short) hashBitLength);
//    	  
//    	  buf.putShort((short) lengthMSB);
//    	  
////          O_hashKeys = buf.pos();
//
//			if (nkeys > 0) {
//
//				final int byteLength = BytesUtil
//						.bitFlagByteLength((lengthMSB + (nkeys * lengthLSB))/* nbits */);
//
//				final byte[] a = new byte[byteLength];
//
//				final OutputBitStream obs = new OutputBitStream(a);
//
//				try {
//
//					// The hash of the first key.
//					int h = ((IBucketData) leaf).getHash(0/* index */);
//
//					// Drop off the LSB bits, leaving the MSB bits in the LSB position.
//					h = h >>> lengthLSB;
//
////					// Reverse bits to since obs writes the LSB of the int.
////					h = Integer.reverse(h);
//					
//					// The MSB prefix.
//					obs.writeInt(h, lengthMSB/* MSB bits */);
//
//					// The LSB of the hash of each key.
//					for (int i = 0; i < nkeys; i++) {
//
//						// The hash of this key.
//						h = ((IBucketData)leaf).getHash(i);
//						
//						// Drop off the MSB bits.
//						h = h >>> lengthMSB;
//						
////						// Reverse bits since obs writes the LSB of the int.
////						h = Integer.reverse(h);
//
//						// The LSB.
//						obs.writeInt(h, lengthLSB);
//
//					}
//
//					// copy onto the buffer.
//					buf.put(a);
//
//				} catch (IOException e) {
//					throw new RuntimeException(e);
//					// Note: close is not necessary if flushed and backed by
//					// byte[].
//					// } finally {
//					// try {
//					// obs.close();
//					// } catch (IOException e) {
//					// log.error(e);
//					// }
//				}
//
//			}
//
////      } else {
////      
////          O_hashKeys = -1;
//          
//      }
      
        // Slice containing the coded leaf.
        final AbstractFixedByteArrayBuffer slice = buf.slice(//
                O_origin, buf.pos() - O_origin);

        // A read-only view of the coded leaf data record.
        return new ReadOnlyLeafData(slice, encodedKeys, encodedValues);
        
    }

    public AbstractFixedByteArrayBuffer encode(final ILeafData leaf,
            final DataOutputBuffer buf) {

        return encodeLive(leaf, buf).data();

    }

	/**
	 * A read-only view of the data for a B+Tree leaf based on a compact record
	 * format. While some fields are cached, for the most part the various data
	 * fields, including the keys and values, are accessed in place in the data
	 * record in order to minimize the memory footprint of the leaf. The keys
	 * and values are coded using a caller specified {@link IRabaCoder}. The
	 * specific coding scheme is specified by the {@link IndexMetadata} for the
	 * B+Tree instance and is not stored within the leaf data record. The use of
	 * prefix coding for keys is a good general choices, but should not be used
	 * in combination with a hash tree unless an order preserving hashing
	 * function is being used.
	 * <p>
	 * Note: The leading byte of the record format codes for a leaf, a
	 * double-linked leaf or a node in a manner which is compatible with
	 * {@link ReadOnlyNodeData}.
	 * 
	 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
	 *         Thompson</a>
	 * @version $Id: DefaultLeafCoder.java 3991 2010-12-03 18:48:02Z thompsonbry
	 *          $
	 */
//	 * <p>
//	 * The {@link DefaultLeafCoder} automatically maintains hash values for keys
//	 * for an {@link IBucketData} record. The hash values of the keys in the
//	 * bucket will have a shared prefix (the MSB hash prefix) which corresponds
//	 * to the globalDepth of the path through the hash tree leading to this
//	 * bucket less the localDepth of this bucket. It is therefore possible to
//	 * store only the LSB bits of the hash values in the page and reconstruct
//	 * the hash values using the MSB bits from the path through the hash tree.
//	 * In order to be able to reconstruct the full hash code key based solely on
//	 * local information, the MSB bits can be written out once and the LSB bits
//	 * can be written out once per tuple. Testing the hash value of a key may
//	 * then be done considering only the LSB bits of the hash value. This
//	 * storage scheme also has the advantage that the hash value is not
//	 * restricted to an int32 and is therefore compatible with the use of
//	 * cryptographic hash functions. (If hash values are stored in a B+Tree leaf
//	 * they will not shared this prefix property and can not be compressed in
//	 * this manner).
	 static private class ReadOnlyLeafData extends AbstractReadOnlyNodeData<ILeafData>
            implements ILeafData {//, IBucketData {

        /** The backing buffer. */
        private final AbstractFixedByteArrayBuffer b;
        
        // fields which are cached by the ctor.
//        private final boolean doubleLinked;
        private final int nkeys;
        private final short flags;
        private final IRaba keys;
        private final IRaba vals;
        
        /**
         * Offset of the bit flags in the buffer encoding the presence of deleted
         * tuples -or- <code>-1</code> if the leaf does not report those data.
         */
        private final int O_deleteMarkers;

        /**
         * The byte offset of the minimum version timestamp in the buffer -or-
         * <code>-1</code> if the leaf does not report those data. The minimum
         * timestamp is coded as a full length long. The next field in the
         * buffer is the maximum version timestamp. The following fields are an
         * array of {@link #nkeys} coded timestamp values (one per tuple). Those
         * timestamps are coded in {@link #versionTimestampBits} each.
         */
        private final int O_versionTimestamps;

        /**
         * The #of bits used to code the version timestamps -or- ZERO (0) if
         * they are not present.
         */
        private final int versionTimestampBits;

		/**
		 * Offset of the bit flags in the buffer encoding the presence of tuples
		 * with raw records -or- <code>-1</code> if the leaf does not report
		 * those data.
		 */
        private final int O_rawRecords;
        
//		/**
//		 * Offset of the int32 hash values in the buffer encoding hash value of
//		 * the tuple keys -or- <code>-1</code> if the leaf does not report those
//		 * data.
//		 */
//        private final int O_hashKeys;

//		/**
//		 * The #of bits used to code the hash keys -or- ZERO (0) if they are not
//		 * present. (The length of the MSB hash prefix is 32-lengthLSB.)
//		 */
//        private final int lengthLSB;
//        
//		/**
//		 * The MSB hash prefix shared by all hash codes on this page -or- ZERO
//		 * (0) if hash codes are not present in the page.
//		 */
//        private final int hashMSB;
        
        public final AbstractFixedByteArrayBuffer data() {

            return b;
            
        }

        /**
         * Constructor used when the caller is encoding the {@link ILeafData}.
         * 
         * @param buf
         *            A buffer containing the leaf data.
         */
        protected ReadOnlyLeafData(final AbstractFixedByteArrayBuffer buf,
                final ICodedRaba keys, final ICodedRaba values) {

            if (buf == null)
                throw new IllegalArgumentException();

            if (keys == null)
                throw new IllegalArgumentException();
            
            if (values == null)
                throw new IllegalArgumentException();

            int pos = O_TYPE;
            final byte type = buf.getByte(pos);
            pos += SIZEOF_TYPE;

            final boolean doubleLinked;
            switch (type) {
            case NODE:
                throw new AssertionError();
            case LEAF:
                doubleLinked = false;
                break;
            case LINKED_LEAF:
                doubleLinked = true;
                break;
            default:
                throw new AssertionError("type=" + type);
            }

            if (doubleLinked) {
                
                // skip over the prior/next addr.
                pos += SIZEOF_ADDR * 2;
                
            }
            
            final int version = buf.getShort(pos);
            pos += SIZEOF_VERSION;
            switch (version) {
            case VERSION0:
                break;
            default:
                throw new AssertionError("version=" + version);
            }

            flags = buf.getShort(pos);
            pos += SIZEOF_FLAGS;
            final boolean hasVersionTimestamps = ((flags & FLAG_VERSION_TIMESTAMPS) != 0);
            final boolean hasDeleteMarkers = ((flags & FLAG_DELETE_MARKERS) != 0);
            final boolean hasRawRecords = ((flags & FLAG_RAW_RECORDS) != 0);
//			final boolean hasHashKeys = ((flags & FLAG_HASH_KEYS) != 0);

            this.nkeys = buf.getInt(pos);
            pos += SIZEOF_NKEYS;

            final int keysSize = buf.getInt(pos);
            pos += SIZEOF_KEYS_SIZE;
            
            final int valuesSize = buf.getInt(pos);
            pos += SIZEOF_KEYS_SIZE;

            // keys
            this.keys = keys;//keysCoder.decode(buf.slice(pos, keysSize));
            pos += keysSize;// skip over the keys.

            // values
            this.vals = values;//valuesCoder.decode(buf.slice(pos,valuesSize));
            pos += valuesSize;// skip over the values.

            // delete markers
            if (hasDeleteMarkers) {
                
                O_deleteMarkers = pos;

                // advance past the bit flags.
                pos += BytesUtil.bitFlagByteLength(nkeys);// bit coded.
                
            } else {
                
                O_deleteMarkers = -1;
                
            }

            // version timestamps
            if (hasVersionTimestamps) {

                versionTimestampBits = buf.getByte(pos);
                pos++;

                O_versionTimestamps = pos;
                // advance past the timestamps.
                pos += (2 * SIZEOF_TIMESTAMP)
                        + BytesUtil.bitFlagByteLength(nkeys
                                * versionTimestampBits/* nbits */);

//                // advance past the timestamps.
//                pos += nkeys * SIZEOF_TIMESTAMP;
                
            } else {
                
                O_versionTimestamps = -1;
                versionTimestampBits = 0;
                
            }

            // raw record flags
            if (hasRawRecords) {
                
                O_rawRecords = pos;

                // advance past the bit flags.
                pos += BytesUtil.bitFlagByteLength(nkeys);// bit coded.
                
            } else {
                
                O_rawRecords = -1;
                
            }
            
//            if(hasHashKeys) {
//
//				final int lengthMSB = buf.getShort(pos);
//				pos += 2;
//
//				lengthLSB = 32 /* hashBitLength */- lengthMSB;
//
//				/*
//				 * The byte offset to the start of the bit coded hash keys. The
//				 * first bit coded value is the MSB prefix. You need to skip
//				 * over that when indexing into the LSB array.
//				 */
//				O_hashKeys = pos;
//
//				final int byteLength = BytesUtil
//						.bitFlagByteLength((lengthMSB + (nkeys * lengthLSB))/* nbits */);
//
//				if (nkeys > 0) {
//					
//					final InputBitStream ibs = buf.slice(pos, byteLength)
//							.getInputBitStream();
//
//					try {
//						hashMSB = ibs.readInt(lengthMSB);
//					} catch (IOException ex) {
//						// Note: should not be thrown.
//						throw new RuntimeException(ex);
//					}
//					
//				} else {
//					
//					hashMSB = 0;
//					
//				}
//            	
//            } else {
//            	
//            	O_hashKeys = -1;
//            	lengthLSB = 0;
//            	hashMSB = 0;
//            	
//            }
            
            // save reference to buffer
            this.b = buf;

        }

        /**
         * Decode in place (wraps a record containing the encoded data for a leaf).
         * 
         * @param buf
         *            A buffer containing the leaf data.
         */
        protected ReadOnlyLeafData(final AbstractFixedByteArrayBuffer buf,
                final IRabaCoder keysCoder, final IRabaCoder valuesCoder) {

            if (buf == null)
                throw new IllegalArgumentException();

            if (keysCoder == null)
                throw new IllegalArgumentException();
            
            if (valuesCoder == null)
                throw new IllegalArgumentException();

            int pos = O_TYPE;
            final byte type = buf.getByte(pos);
            pos += SIZEOF_TYPE;

            final boolean doubleLinked;
            switch (type) {
            case NODE:
                throw new AssertionError();
            case LEAF:
                doubleLinked = false;
                break;
            case LINKED_LEAF:
                doubleLinked = true;
                break;
            default:
                throw new AssertionError("type=" + type);
            }

            if (doubleLinked) {
                
                // skip over the prior/next addr.
                pos += SIZEOF_ADDR * 2;
                
            }
            
            final int version = buf.getShort(pos);
            pos += SIZEOF_VERSION;
            switch (version) {
            case VERSION0:
                break;
            default:
                throw new AssertionError("version=" + version);
            }

            flags = buf.getShort(pos);
            pos += SIZEOF_FLAGS;
            final boolean hasVersionTimestamps = ((flags & FLAG_VERSION_TIMESTAMPS) != 0);
            final boolean hasDeleteMarkers = ((flags & FLAG_DELETE_MARKERS) != 0);
            final boolean hasRawRecords = ((flags & FLAG_RAW_RECORDS) != 0);
//			final boolean hasHashKeys = ((flags & FLAG_HASH_KEYS) != 0);

            this.nkeys = buf.getInt(pos);
            pos += SIZEOF_NKEYS;

            final int keysSize = buf.getInt(pos);
            pos += SIZEOF_KEYS_SIZE;
            
            final int valuesSize = buf.getInt(pos);
            pos += SIZEOF_KEYS_SIZE;

            // keys
            this.keys = keysCoder.decode(buf.slice(pos, keysSize));
            pos += keysSize;// skip over the keys.

            // values
            this.vals = valuesCoder.decode(buf.slice(pos,valuesSize));
            pos += valuesSize;// skip over the values.

            // delete markers
            if (hasDeleteMarkers) {
                
                O_deleteMarkers = pos;

                // advance past the bit flags.
                pos += BytesUtil.bitFlagByteLength(nkeys);// bit coded.
                
            } else {
                
                O_deleteMarkers = -1;
                
            }

            // version timestamps
            if (hasVersionTimestamps) {

                versionTimestampBits = buf.getByte(pos);
                pos++;

                O_versionTimestamps = pos;
                // advance past the timestamps.
                pos += (2 * SIZEOF_TIMESTAMP)
                        + BytesUtil.bitFlagByteLength(nkeys
                                * versionTimestampBits/* nbits */);

//                // advance past the timestamps.
//                pos += nkeys * SIZEOF_TIMESTAMP;
                
            } else {
                
                O_versionTimestamps = -1;
                versionTimestampBits = 0;
                
            }

            // raw record markers
            if (hasRawRecords) {
                
                O_rawRecords = pos;

                // advance past the bit flags.
                pos += BytesUtil.bitFlagByteLength(nkeys);// bit coded.
                
            } else {
                
                O_rawRecords = -1;
                
            }

//            if(hasHashKeys) {
//
//				final int lengthMSB = buf.getShort(pos);
//				pos += 2;
//
//				lengthLSB = 32 /* hashBitLength */- lengthMSB;
//
//				/*
//				 * The byte offset to the start of the bit coded hash keys. The
//				 * first bit coded value is the MSB prefix. You need to skip
//				 * over that when indexing into the LSB array.
//				 */
//				O_hashKeys = pos;
//
//				final int byteLength = BytesUtil
//						.bitFlagByteLength((lengthMSB + (nkeys * lengthLSB))/* nbits */);
//
//				if (nkeys > 0) {
//					
//					final InputBitStream ibs = buf.slice(pos, byteLength)
//							.getInputBitStream();
//
//					try {
//						hashMSB = ibs.readInt(lengthMSB);
//					} catch (IOException ex) {
//						// Note: should not be thrown.
//						throw new RuntimeException(ex);
//					}
//					
//				} else {
//					
//					hashMSB = 0;
//					
//				}
//            	
//            } else {
//            	
//            	O_hashKeys = -1;
//            	lengthLSB = 0;
//            	hashMSB = 0;
//            	
//            }

            // save reference to buffer
            this.b = buf;

        }

        /**
         * Always returns <code>true</code>.
         */
        final public boolean isLeaf() {

            return true;

        }

        /**
         * Yes.
         */
        final public boolean isReadOnly() {
            
            return true;
            
        }

        /**
         * Yes.
         */
        final public boolean isCoded() {
            
            return true;
            
        }

        /**
         * {@inheritDoc}. This field is cached.
         */
        final public int getKeyCount() {

            return nkeys;

        }

        /**
         * For a leaf the #of tuples is always the #of keys.
         */
        final public int getSpannedTupleCount() {
            
            return nkeys;
            
        }

        /**
         * For a leaf, the #of values is always the #of keys.
         */
        final public int getValueCount() {
            
            return nkeys;
            
        }
        
        final public boolean hasVersionTimestamps() {
            
            return (flags & FLAG_VERSION_TIMESTAMPS) != 0;
            
        }

        final public boolean hasDeleteMarkers() {

            return (flags & FLAG_DELETE_MARKERS) != 0;
            
        }

        final public boolean hasRawRecords() {

            return (flags & FLAG_RAW_RECORDS) != 0;
            
        }

//        final public boolean hasHashKeys() {
//
//            return (flags & FLAG_HASH_KEYS) != 0;
//            
//        }

        public long getMinimumVersionTimestamp() {

            if (!hasVersionTimestamps())
                throw new UnsupportedOperationException();

            return b.getLong(O_versionTimestamps);

        }

        public long getMaximumVersionTimestamp() {

            if (!hasVersionTimestamps())
                throw new UnsupportedOperationException();

            return b.getLong(O_versionTimestamps + SIZEOF_TIMESTAMP);

        }

        final public long getVersionTimestamp(final int index) {

            if (!hasVersionTimestamps())
                throw new UnsupportedOperationException();

//            return b.getLong(O_versionTimestamps + index * SIZEOF_TIMESTAMP);

            final InputBitStream ibs = b.getInputBitStream();
            try {

                final long bitpos = ((O_versionTimestamps + (2L * SIZEOF_TIMESTAMP)) << 3)
                        + ((long)index * versionTimestampBits);

                ibs.position(bitpos);

                final long deltat = ibs
                        .readLong(versionTimestampBits/* nbits */);

                return getMinimumVersionTimestamp() + deltat;
                
            } catch(IOException ex) {
                
                throw new RuntimeException(ex);
                
// close not required for IBS backed by byte[] and has high overhead.
//            } finally {
//                try {
//                    ibs.close();
//                } catch (IOException ex) {
//                    log.error(ex);
//                }
            }

        }

        final public boolean getDeleteMarker(final int index) {

            if (!hasDeleteMarkers())
                throw new UnsupportedOperationException();

            return b.getBit((O_deleteMarkers << 3) + index);

        }

        final public long getRawRecord(final int index) {

            if (!hasRawRecords())
                throw new UnsupportedOperationException();

			final boolean flag = b.getBit((O_rawRecords << 3) + index);

			if (!flag)
				return IRawStore.NULL;

			/*
			 * @todo This could be optimized to decode without materializing the
			 * byte[8] array which represents the long addr.
			 */
			return AbstractBTree.decodeRecordAddr(vals.get(index));
			
        }

//        final public int getLengthMSB() {
//        	
//			if (!hasHashKeys())
//				throw new UnsupportedOperationException();
//
//			final int lengthMSB = 32/* hashBitLength */- lengthLSB;
//
//			return lengthMSB;
//        	
//        }
        
//		final public int getHash(final int index) {
//
//            if (index < 0 || index >= nkeys)
//                throw new IllegalArgumentException();
//            
//			if (!hasHashKeys())
//				throw new UnsupportedOperationException();
//
//			final int lengthMSB = 32/* hashBitLength */- lengthLSB;
//
//			final int byteLength = BytesUtil.bitFlagByteLength(lengthMSB
//					+ (nkeys * lengthLSB)/* nbits */);
//
//			final InputBitStream ibs = b.slice(O_hashKeys, byteLength)
//					.getInputBitStream();
//
//			try {
//
//				final long position = lengthMSB + index * lengthLSB;
//
//				ibs.position(position);
//				
//				int h = ibs.readInt(lengthLSB);
//				
//				h |= hashMSB;
//        		
//				return h;
//				
//        	} catch(IOException ex) {
//        		
//        		throw new RuntimeException(ex);
//        		
//        	}
//        	
//        }
//
//		public Iterator<Integer> hashIterator(final int h) {
//			
//			return new HashMatchIterator(h);
//			
//		}

//		/**
//		 * Visits the index of each bucket entry having a matching hash code.
//		 * 
//		 * @todo a trie over the hash entries would provide much faster search.
//		 */
//		private class HashMatchIterator implements Iterator<Integer> {
//			
//			private final int h;
//			private final int lengthMSB;
//			private final InputBitStream ibs;
//			private int currentIndex = 0;
//			private Integer nextResult = null;
//
//			private HashMatchIterator(final int h) {
//
//				this.h = h;
//				
//				lengthMSB = 32/* hashBitLength */- lengthLSB;
//
//				final int byteLength = BytesUtil.bitFlagByteLength(lengthMSB
//						+ (nkeys * lengthLSB)/* nbits */);
//
//				ibs = b.slice(O_hashKeys, byteLength)
//						.getInputBitStream();
//
//			}
//
//			public boolean hasNext() {
//
//				final int n = getKeyCount();
//
//				while (nextResult == null && currentIndex < n) {
//
//					final int index = currentIndex++;
//					
//					int h1;
//					try {
//
//						// We do not need to re-position the ibs.
////						final long position = lengthMSB + currentIndex
////								* lengthLSB;
////						ibs.position(position);
//
//						h1 = ibs.readInt(lengthLSB);
//
//						h1 |= hashMSB;
//
//					} catch (IOException ex) {
//
//						throw new RuntimeException(ex);
//
//					}
//
//					if (h1 == h) {
//
//						nextResult = Integer.valueOf(index);
//
//						break;
//
//					}
//					
//				}
//
//				return nextResult != null;
//				
//			}
//
//			public Integer next() {
//				
//				if (!hasNext())
//					throw new NoSuchElementException();
//				
//				final Integer tmp = nextResult;
//				
//				nextResult = null;
//				
//				return tmp;
//				
//			}
//
//			public void remove() {
//				
//				throw new UnsupportedOperationException();
//				
//			}
//
//		}

		final public IRaba getKeys() {
            
            return keys;
            
        }

        final public IRaba getValues() {

            return vals;
            
        }

        public String toString() {

            final StringBuilder sb = new StringBuilder();

            sb.append(getClass().getName() + "{");

            DefaultLeafCoder.toString(this, sb);

            sb.append("}");
            
            return sb.toString();
            
        }

        /*
         * Double-linked leaf support.
         */
        
        /**
         * Return <code>true</code> if the leaf encodes the address or the prior and
         * next leaves.
         */
        public final boolean isDoubleLinked() {

            return b.getByte(0) == LINKED_LEAF;

        }

//        /**
//         * Update the data record to set the prior and next leaf address.
//         * <p>
//         * Note: In order to use this method to write linked leaves on the store
//         * you have to either write behind at a pre-determined address on the
//         * store or settle for writing only the prior or the next leaf address,
//         * but not both. It is up to the caller to perform these tricks. All
//         * this method does is to touch up the serialized record.
//         * <p>
//         * Note: This method has NO side-effects on the <i>position</i> or
//         * <i>limit</i> of the internal {@link ByteBuffer}.
//         * 
//         * @param priorAddr
//         *            The address of the previous leaf in key order,
//         *            <code>0L</code> if it is known that there is no previous
//         *            leaf, and <code>-1L</code> if either: (a) it is not known
//         *            whether there is a previous leaf; or (b) it is known but
//         *            the address of that leaf is not known to the caller.
//         * @param nextAddr
//         *            The address of the next leaf in key order, <code>0L</code>
//         *            if it is known that there is no next leaf, and
//         *            <code>-1L</code> if either: (a) it is not known whether
//         *            there is a next leaf; or (b) it is known but the address
//         *            of that leaf is not known to the caller.
//         * 
//         * @see IndexSegmentBuilder
//         */
//        public void updateLeaf(final long priorAddr, final long nextAddr) {
//
//            if (!isDoubleLinked()) {
//
//                // Not double-linked.
//                throw new UnsupportedOperationException();
//
//            }
//
//            /*
//             * Note: these fields are written immediately after the byte
//             * indicating whether this is a leaf, linked-leaf, or node.
//             */
//
//            b.putLong(O_PRIOR, priorAddr);
//
//            b.putLong(O_NEXT + SIZEOF_ADDR, nextAddr);
//
//        }

        public final long getPriorAddr() {
            
            if(!isDoubleLinked())
                throw new UnsupportedOperationException();

            return b.getLong(O_PRIOR);
            
        }
        
        public final long getNextAddr() {
            
            if(!isDoubleLinked())
                throw new UnsupportedOperationException();

            return b.getLong(O_NEXT);
            
        }

    }

    /**
     * Utility method formats the {@link ILeafData}.
     * 
     * @param leaf
     *            A leaf data record.
     * @param sb
     *            The representation will be written onto this object.
     * 
     * @return The <i>sb</i> parameter.
     */
    static public StringBuilder toString(final ILeafData leaf,
            final StringBuilder sb) {

        final int nkeys = leaf.getKeyCount();

        if(leaf.isDoubleLinked()) {
            
            sb.append(", priorAddr=" + leaf.getPriorAddr());

            sb.append(", nextAddr=" + leaf.getNextAddr());

        }
        
        sb.append(",\nkeys=" + leaf.getKeys());
        
        sb.append(",\nvals=" + leaf.getValues());

        if (leaf.hasDeleteMarkers()) {

            sb.append(",\ndeleteMarkers=[");

            for (int i = 0; i < nkeys; i++) {

                if (i > 0)
                    sb.append(", ");

                sb.append(leaf.getDeleteMarker(i));

            }

            sb.append("]");

        }

        if (leaf.hasVersionTimestamps()) {

            sb.append(",\nversionTimestamps={min="
                    + leaf.getMinimumVersionTimestamp() + ",max="
                    + leaf.getMaximumVersionTimestamp() + ",tuples=[");

            for (int i = 0; i < nkeys; i++) {

                if (i > 0)
                    sb.append(", ");

                // sb.append(new Date(leaf.getVersionTimestamp(i)).toString());
                sb.append(leaf.getVersionTimestamp(i));

            }

            sb.append("]");

        }

        if (leaf.hasRawRecords()) {

            sb.append(",\nrawRecords=[");

            for (int i = 0; i < nkeys; i++) {

                if (i > 0)
                    sb.append(", ");

                sb.append(leaf.getRawRecord(i));

            }

            sb.append("]");

        }


//        if (leaf instanceof IBucketData) {
//
//        	final IBucketData d = (IBucketData)leaf;
//        	
//			sb.append(",\nhashCodes={lengthMSB=" + d.getLengthMSB()
//					+ ",tuples=[");
//
//            for (int i = 0; i < nkeys; i++) {
//
//                if (i > 0)
//                    sb.append(", ");
//
//                sb.append(d.getHash(i));
//
//            }
//
//            sb.append("]");
//
//        }

        return sb;

    }

}
