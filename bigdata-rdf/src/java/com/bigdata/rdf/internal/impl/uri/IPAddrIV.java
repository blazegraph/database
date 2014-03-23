/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
package com.bigdata.rdf.internal.impl.uri;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.btree.BytesUtil.UnsignedByteArrayComparator;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.io.LongPacker;
import com.bigdata.rdf.internal.DTE;
import com.bigdata.rdf.internal.ILexiconConfiguration;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.AbstractInlineIV;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.spo.SPOKeyOrder;

/**
 * Internal value representing an inline IP address.  Uses the InetAddress
 * class to represent the IP address and perform the translation to and from
 * byte[], which is then used directly in the IV key (after the flags).
 * <p>
 * This internal value has a {@link VTE} of {@link VTE#URI}.
 * <p>
 * {@inheritDoc}
 */
public class IPAddrIV<V extends BigdataURI> extends AbstractInlineIV<V, InetAddress>
        implements Serializable, URI {

    /**
	 * 
	 */
	private static final long serialVersionUID = 685148537376856907L;
	
//	private static final transient Logger log = Logger.getLogger(SidIV.class);

	public static final String NAMESPACE = "ip:/";
	
	public static final int NAMESPACE_LEN = NAMESPACE.length();
	
	/**
	 * The inline IP address.
	 */
	private final InetAddress value;
	
	/**
	 * The cached string representation of this IP.
	 */
	private transient String hostAddress;
	
//	/**
//	 * The IPv4 prefix byte.
//	 */
//	private transient byte prefix;
	
	/**
	 * The cached byte[] key for the encoding of this IV.
	 */
	private transient byte[] key;
	
	/**
	 * The cached materialized BigdataValue for this InetAddress.
	 */
	private transient V uri;

    public IV<V, InetAddress> clone(final boolean clearCache) {

        final IPAddrIV<V> tmp = new IPAddrIV<V>(value);//, prefix);

        // Propagate the cached byte[] key.
        tmp.key = key;
        
        // Propagate the cached BigdataValue.
        tmp.uri = uri;
        
        if (!clearCache) {

            tmp.setValue(getValueCache());
            
        }
        
        return tmp;

    }

    /**
	 * Ctor with internal value specified.
	 */
	public IPAddrIV(final InetAddress value) {//, final byte prefix) {

        /*
         * TODO Using XSDBoolean so that we can know how to decode this thing
         * as an IPAddrIV.  We need to fix the Extension mechanism for URIs. 
         */
        super(VTE.URI, DTE.XSDBoolean);
        
        this.value = value;
        
//        this.prefix = prefix;
        
    }

	/*
	 * Somebody please fix this for the love of god.
	 */
	public static final Pattern pattern = 
			Pattern.compile("((?:[0-9]{1,3}\\.){3}[0-9]{1,3})((\\/)(([0-9]{1,2})))?");
	
    /**
	 * Ctor with host address specified.
	 */
	public IPAddrIV(final String hostAddress) throws UnknownHostException {

        /*
         * Note: XSDBoolean happens to be assigned the code value of 0, which is
         * the value we we want when the data type enumeration will be ignored.
         */
        super(VTE.URI, DTE.XSDBoolean);
        
        this.hostAddress = hostAddress;
        
		final Matcher matcher = pattern.matcher(hostAddress);
		
		final boolean matches = matcher.matches();
		
		if (matches) {
		
			final String ip = matcher.group(1);
			
//			log.debug(ip);
			
			this.value = InetAddress.getByName(ip);
			
//			final String suffix = matcher.group(4);
			
//			log.debug(suffix);
			
//			this.prefix = suffix != null ? Byte.valueOf(suffix) : (byte) 33;
			
		} else {
			
			throw new IllegalArgumentException("not an IP: " + hostAddress);
			
//			log.debug("no match");
			
		}
        
    }

	/**
	 * Returns the inline value.
	 */
	public InetAddress getInlineValue() throws UnsupportedOperationException {
		return value;
	}

	/**
	 * Returns the URI representation of this IV.
	 */
    public V asValue(final LexiconRelation lex) {
    	if (uri == null) {
	        uri = (V) lex.getValueFactory().createURI(getNamespace(), getLocalName());
	        uri.setIV(this);
    	}
        return uri;
    }

    /**
     * Return the byte length for the byte[] encoded representation of this
     * internal value.  Depends on the byte length of the encoded inline value.
     */
	public int byteLength() {
		return 1 + key().length;
	}

	public String toString() {
		return "IP("+getLocalName()+")";
	}
	
	public int hashCode() {
		return value.hashCode();
	}

//    /**
//     * Implements {@link BNode#getID()}.
//     * <p>
//     * This implementation uses the {@link BigInteger} class to create a unique
//     * blank node ID based on the <code>unsigned byte[]</code> key of the inline
//     * {@link SPO}.
//     */
//	@Override
//	public String getID() {
////		// just use the hash code.  can result in collisions
////		return String.valueOf(hashCode());
//		
//		// create a big integer using the spo key.  should result in unique ids
//		final byte[] key = key();
//		final int signum = key.length > 0 ? 1 : 0;
//		final BigInteger bi = new BigInteger(signum, key);
//		return 's' + bi.toString();
//	}

	@Override
	public String getNamespace() {
		return NAMESPACE;
	}
	
	@Override
	public String getLocalName() {
		if (hostAddress == null) {
			
//			if (prefix < 33) {
//				hostAddress = value.getHostAddress() + "/" + prefix;
//			} else {
				hostAddress = value.getHostAddress();
//			}
			
		}
		return hostAddress;
	}
	
	/**
	 * Two {@link IPAddrIV} are equal if their InetAddresses are equal.
	 */
	public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o instanceof IPAddrIV) {
        		final InetAddress value2 = ((IPAddrIV<?>) o).value;
//        		final byte prefix2 = ((IPAddrIV<?>) o).prefix;
        		return value.equals(value2);// && prefix == prefix2;
        }
        return false;
	}

	public int _compareTo(IV o) {

	    /*
	     * Note: This works, but it might be more expensive.
	     */
	    return UnsignedByteArrayComparator.INSTANCE.compare(key(), ((IPAddrIV)o).key());

	}
	
    /**
     * Encode this internal value into the supplied key builder.  Emits the
     * flags, following by the encoded byte[] representing the spo, in SPO
     * key order.
     * <p>
     * {@inheritDoc}
     */
	@Override
    public IKeyBuilder encode(final IKeyBuilder keyBuilder) {

        // First emit the flags byte.
        keyBuilder.appendSigned(flags());
		
		// Then append the InetAddress byte[] and the prefix.
        keyBuilder.append(key());
        
        return keyBuilder;
            
    }
    
    private byte[] key() {

        if (key == null) {
        
//        	final IKeyBuilder kb = KeyBuilder.newInstance();
//        	
//        	kb.append(value.getAddress());
//        	
//        	kb.append(prefix);
//        	
//        	key = kb.getKey();

        	key = value.getAddress();
        	
        }

        return key;
        
    }

    /**
     * Object provides serialization for {@link IPAddrIV} via the write-replace
     * and read-replace pattern.
     */
    private static class IPAddrIVState implements Externalizable {

        private static final long serialVersionUID = -1L;

//        private byte flags;
        private byte[] key;
        
        /**
         * De-serialization constructor.
         */
        public IPAddrIVState() {
            
        }
        
        private IPAddrIVState(final IPAddrIV iv) {
//            this.flags = flags;
            this.key = iv.key();
        }
        
        public void readExternal(ObjectInput in) throws IOException,
                ClassNotFoundException {
//            flags = in.readByte();
            final int nbytes = LongPacker.unpackInt(in);
            key = new byte[nbytes];
            in.readFully(key);
        }

        public void writeExternal(ObjectOutput out) throws IOException {
//            out.writeByte(flags);
            LongPacker.packLong(out, key.length);
            out.write(key);
        }
        
        private Object readResolve() throws ObjectStreamException {

        	try {
        		
	            final InetAddress value = InetAddress.getByAddress(key);
	            
	            return new IPAddrIV(value);
	            
        	} catch (UnknownHostException ex) {
        		
        		throw new RuntimeException(ex);
        		
        	}
            
        }

    }
    
    private Object writeReplace() throws ObjectStreamException {
        
        return new IPAddrIVState(this);
        
    }
    
    /**
     * Implements {@link Value#stringValue()}.
     */
    @Override
    public String stringValue() {
        
        return getLocalName();

    }

    /**
     * Does not need materialization to answer URI interface methods.
     */
	@Override
	public boolean needsMaterialization() {
		
		return false;
		
	}


}