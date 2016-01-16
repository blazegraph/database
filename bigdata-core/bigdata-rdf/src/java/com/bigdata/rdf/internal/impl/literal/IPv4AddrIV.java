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
package com.bigdata.rdf.internal.impl.literal;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;

import com.bigdata.rdf.internal.DTE;
import com.bigdata.rdf.internal.DTEExtension;
import com.bigdata.rdf.internal.IPv4Address;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.util.BytesUtil;

/**
 * Internal value representing an inline IP address. Uses the IPv4Address class
 * to represent the IP address and perform the translation to and from a
 * <code>byte[]</code>, which is then used directly in the IV key (after the
 * flags).
 * <p>
 * {@inheritDoc}
 * <p>
 * Note: Binary compatibility for this class was broken by BLZG-1507.
 * 
 * @see BLZG-1507 (Implement support for DTE extension types for URIs)
 */
public class IPv4AddrIV<V extends BigdataLiteral> 
        extends AbstractLiteralIV<V, IPv4Address>
            implements Serializable, Literal {

    /**
	 * 
	 */
	private static final long serialVersionUID = 685148537376856907L;
	
	private static final transient Logger log = Logger.getLogger(IPv4AddrIV.class);

	/**
	 * The inline IP address.
	 */
	private final IPv4Address value;
	
	/**
	 * The cached string representation of this IP.
	 */
	private transient String hostAddress;
	
	/**
	 * The cached materialized BigdataValue for this InetAddress.
	 */
	private transient V uri;

	@Override
    public IV<V, IPv4Address> clone(final boolean clearCache) {

        final IPv4AddrIV<V> tmp = new IPv4AddrIV<V>(value);

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
	public IPv4AddrIV(final IPv4Address value) {

        super(DTE.Extension);
        
        this.value = value;
        
    }

	/**
	 * Regex pattern for IPv4 Address with optional CIDR
	 */
	private static transient final String IPv4_OPTIONAL_CIDR_PATTERN = "((([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5]))(\\/([012]?\\d|3[012]))?";
	
	// "((?:[0-9]{1,3}\\.){3}[0-9]{1,3})((\\/)(([0-9]{1,2})))?

	private static transient final Pattern pattern = 
			Pattern.compile(IPv4_OPTIONAL_CIDR_PATTERN);
	
    /**
	 * Ctor with host address specified.
	 * 
	 * @throws UnknownHostException
	 *             if not parsable as an IPv4 address.
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
			
			final String suffix = matcher.group(6);
			
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
			
			this.value = IPv4Address.IPv4Factory(s);
			
			if (value == null) {
			    if (log.isDebugEnabled()) {
			        log.debug("not a valid IP: " + hostAddress);
			    }
	            throw new UnknownHostException("not a valid IP: " + hostAddress);
			}
			
	        if (log.isDebugEnabled()) {
                log.debug(value);
                log.debug(byteLength());
                log.debug(BytesUtil.toString(value.getBytes()));
    		}
	        
		} else {
			
            if (log.isDebugEnabled()) {
                log.debug("not a valid IP: " + hostAddress);
            }
			throw new UnknownHostException("Did not match REGEX - not a valid IP: " + hostAddress);
			
		}
        
    }

	@Override
    public DTEExtension getDTEX() {
        return DTEExtension.IPV4;
    }
    
    @Override
	public IPv4Address getInlineValue() throws UnsupportedOperationException {
		return value;
	}

	@Override
    @SuppressWarnings("unchecked")
    public V asValue(final LexiconRelation lex) {
    	if (uri == null) {
	        uri = (V) lex.getValueFactory().createLiteral(getLabel(), XSD.IPV4);
	        uri.setIV(this);
    	}
        return uri;
    }

	@Override
	public int byteLength() {
        return 1 /* flags */ + 1 /* DTEExtension */ + value.getBytes().length;
	}

	@Override
	public String toString() {
		return "IPv4(" + getLabel() + ")";
	}
	
	@Override
	public int hashCode() {
		return value.hashCode();
	}

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
    @Override
	public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o instanceof IPv4AddrIV) {
        		final IPv4Address value2 = ((IPv4AddrIV<?>) o).value;
        		return value.equals(value2);
        }
        return false;
	}

        @Override
	@SuppressWarnings("rawtypes")
	public int _compareTo(final IV o) {
        return value.compareTo(((IPv4AddrIV) o).value);
    }
    
    public static Matcher getIPv4Matcher(String addr) {
    	return IPv4AddrIV.pattern.matcher(addr);
    }

}
