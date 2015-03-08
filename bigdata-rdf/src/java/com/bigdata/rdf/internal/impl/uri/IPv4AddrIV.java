/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.BytesUtil.UnsignedByteArrayComparator;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.io.LongPacker;
import com.bigdata.rdf.internal.DTE;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.Inet4Address;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataLiteral;

/**
 * Internal value representing an inline IP address.  Uses the InetAddress
 * class to represent the IP address and perform the translation to and from
 * byte[], which is then used directly in the IV key (after the flags).
 * <p>
 * This internal value has a {@link VTE} of {@link VTE#URI}.
 * <p>
 * {@inheritDoc}
 */
public class IPv4AddrIV<V extends BigdataLiteral> 
        extends AbstractLiteralIV<V, Inet4Address>
            implements Serializable, Literal {

    /**
	 * 
	 */
	private static final long serialVersionUID = 685148537376856907L;
	
	private static final transient Logger log = Logger.getLogger(IPv4AddrIV.class);

	/**
	 * The inline IP address.
	 */
	private final Inet4Address value;
	
	/**
	 * The cached string representation of this IP.
	 */
	private transient String hostAddress;
	
	/**
	 * The cached byte[] key for the encoding of this IV.
	 */
	private transient byte[] key;
	
	/**
	 * The cached materialized BigdataValue for this InetAddress.
	 */
	private transient V uri;

    public IV<V, Inet4Address> clone(final boolean clearCache) {

        final IPv4AddrIV<V> tmp = new IPv4AddrIV<V>(value);//, prefix);

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
	public IPv4AddrIV(final Inet4Address value) {//, final byte prefix) {

        super(DTE.Extension);
        
        this.value = value;
        
    }

	/*
	 * Somebody please fix this for the love of god.
	 */
	public static final Pattern pattern = 
			Pattern.compile("((?:[0-9]{1,3}\\.){3}[0-9]{1,3})((\\/)(([0-9]{1,2})))?");
	
    /**
	 * Ctor with host address specified.
	 */
	public IPv4AddrIV(final String hostAddress) throws UnknownHostException {

        super(DTE.Extension);
        
        this.hostAddress = hostAddress;
        
		final Matcher matcher = pattern.matcher(hostAddress);
		
		final boolean matches = matcher.matches();
		
		if (matches) {
		
			final String ip = matcher.group(1);
			
			if (log.isDebugEnabled())
			    log.debug(ip);
			
			final String suffix = matcher.group(4);
			
            if (log.isDebugEnabled())
                log.debug(suffix);

			final String[] s;
			if (suffix != null) {
				
				s = new String[5];
				System.arraycopy(ip.split("\\.", -1), 0, s, 0, 4);
				s[4] = suffix;
				
			} else {
				
				s = ip.split("\\.", -1);

			}
			
			this.value = Inet4Address.textToAddr(s);
			
			if (value == null) {
			    if (log.isDebugEnabled()) {
			        log.debug("not a valid IP: " + hostAddress);
			    }
	            throw new UnknownHostException("not a valid IP: " + hostAddress);
			}
			
	        if (log.isDebugEnabled()) {
                log.debug(value);
                log.debug(byteLength());
                log.debug(BytesUtil.toString(key()));
    		}
	        
		} else {
			
            if (log.isDebugEnabled()) {
                log.debug("not a valid IP: " + hostAddress);
            }
			throw new UnknownHostException("not a valid IP: " + hostAddress);
			
		}
        
    }

	/**
	 * Returns the inline value.
	 */
	public Inet4Address getInlineValue() throws UnsupportedOperationException {
		return value;
	}

	/**
	 * Returns the Literal representation of this IV.
	 */
    @SuppressWarnings("unchecked")
    public V asValue(final LexiconRelation lex) {
    	if (uri == null) {
	        uri = (V) lex.getValueFactory().createLiteral(getLabel(), XSD.IPV4);
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
		return "IPv4("+getLabel()+")";
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

//	@Override
//	public String getNamespace() {
//		return NAMESPACE;
//	}
//	
//	@Override
//	public String getLocalName() {
//		if (hostAddress == null) {
//			hostAddress = value.toString();
//		}
//		return hostAddress;
//	}
	
    @Override
    public String getLabel() {
        if (hostAddress == null) {
            hostAddress = value.toString();
        }
        return hostAddress;
    }
    
	/**
	 * Two {@link IPv4AddrIV} are equal if their InetAddresses are equal.
	 */
	public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o instanceof IPv4AddrIV) {
        		final Inet4Address value2 = ((IPv4AddrIV<?>) o).value;
        		return value.equals(value2);
        }
        return false;
	}

	public int _compareTo(IV o) {

	    /*
	     * Note: This works, but it might be more expensive.
	     */
	    return UnsignedByteArrayComparator.INSTANCE.compare(key(), ((IPv4AddrIV)o).key());

	}
	
    /**
     * Encode this internal value into the supplied key builder.  Emits the
     * flags, following by the encoded byte[] representing the IPv4 address.
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
        	key = value.getBytes();
        }

        return key;
        
    }

    /**
     * Object provides serialization for {@link IPv4AddrIV} via the write-replace
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
        
        private IPAddrIVState(final IPv4AddrIV iv) {
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
	        return new Inet4Address(key);
        }

    }
    
    private Object writeReplace() throws ObjectStreamException {
        return new IPAddrIVState(this);
    }
    
//    /**
//     * Implements {@link Value#stringValue()}.
//     */
//    @Override
//    public String stringValue() {
//        
//        return getLocalName();
//
//    }

}
