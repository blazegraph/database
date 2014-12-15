package com.bigdata.rdf.internal;

import java.util.Arrays;

import com.bigdata.btree.BytesUtil.UnsignedByteArrayComparator;

/**
 * This class represents an Internet Protocol version 4 (IPv4) address.
 * Defined by <a href="http://www.ietf.org/rfc/rfc790.txt">
 * <i>RFC&nbsp;790: Assigned Numbers</i></a>,
 * <a href="http://www.ietf.org/rfc/rfc1918.txt">
 * <i>RFC&nbsp;1918: Address Allocation for Private Internets</i></a>,
 * and <a href="http://www.ietf.org/rfc/rfc2365.txt"><i>RFC&nbsp;2365:
 * Administratively Scoped IP Multicast</i></a>
 *
 * <h4> <A NAME="format">Textual representation of IP addresses</a> </h4>
 *
 * Textual representation of IPv4 address used as input to methods
 * takes one of the following forms:
 *
 * <blockquote><table cellpadding=0 cellspacing=0 summary="layout">
 * <tr><td><tt>d.d.d.d</tt></td></tr>
 * <tr><td><tt>d.d.d</tt></td></tr>
 * <tr><td><tt>d.d</tt></td></tr>
 * <tr><td><tt>d</tt></td></tr>
 * </table></blockquote>
 *
 * <p> When four parts are specified, each is interpreted as a byte of
 * data and assigned, from left to right, to the four bytes of an IPv4
 * address.

 * <p> When a three part address is specified, the last part is
 * interpreted as a 16-bit quantity and placed in the right most two
 * bytes of the network address. This makes the three part address
 * format convenient for specifying Class B net- work addresses as
 * 128.net.host.
 *
 * <p> When a two part address is supplied, the last part is
 * interpreted as a 24-bit quantity and placed in the right most three
 * bytes of the network address. This makes the two part address
 * format convenient for specifying Class A network addresses as
 * net.host.
 *
 * <p> When only one part is given, the value is stored directly in
 * the network address without any byte rearrangement.
 *
 * <p> For methods that return a textual representation as output
 * value, the first form, i.e. a dotted-quad string, is used.
 *
 * <h4> The Scope of a Multicast Address </h4>
 *
 * Historically the IPv4 TTL field in the IP header has doubled as a
 * multicast scope field: a TTL of 0 means node-local, 1 means
 * link-local, up through 32 means site-local, up through 64 means
 * region-local, up through 128 means continent-local, and up through
 * 255 are global. However, the administrative scoping is preferred.
 * Please refer to <a href="http://www.ietf.org/rfc/rfc2365.txt">
 * <i>RFC&nbsp;2365: Administratively Scoped IP Multicast</i></a>
 * @since 1.4
 */

public final class Inet4Address {
    
//	private static final Logger log = Logger.getLogger(Inet4Address.class);
	
    final byte[] address;
    
    public Inet4Address(final byte addr[]) {
    	
//    	if (addr == null || addr.length != 5)
//    		throw new IllegalArgumentException();
//    	
//        long address  = addr[4] & 0xFFl;
//        address |= ((addr[3] << 8) & 0xFF00l);
//        address |= ((addr[2] << 16) & 0xFF0000l);
//        address |= ((addr[1] << 24) & 0xFF000000l);
//        address |= (((long)(addr[0] << 32)) & 0xFF00000000l);
        
        this.address = addr;
        
    }

    /**
     * Returns the raw IP address of this <code>InetAddress</code>
     * object. The result is in network byte order: the highest order
     * byte of the address is in <code>getAddress()[0]</code>.
     *
     * @return  the raw IP address of this object.
     */
    public byte[] getBytes() {
    	
//        byte[] addr = new byte[5];
//        addr[0] = (byte) ((address >>> 32) & 0xFF);
//        addr[1] = (byte) ((address >>> 24) & 0xFF);
//        addr[2] = (byte) ((address >>> 16) & 0xFF);
//        addr[3] = (byte) ((address >>> 8) & 0xFF);
//        addr[4] = (byte) (address & 0xFF);
//        
//        return addr;
    	
    	return address;
        
    }

    /**
     * Returns the IP address string in textual presentation form.
     *
     * @return  the raw IP address in a string format.
     * @since   JDK1.0.2
     */
    public String toString() {
        return numericToTextFormat(getBytes());
    }

    @Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(address);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Inet4Address other = (Inet4Address) obj;

		return UnsignedByteArrayComparator.INSTANCE.compare(address, other.address) == 0;
	}

    // Utilities
    /*
     * Converts IPv4 binary address into a string suitable for presentation.
     *
     * @param src a byte array representing an IPv4 numeric address
     * @return a String representing the IPv4 address in
     *         textual representation format
     * @since 1.4
     */

    static String numericToTextFormat(byte[] src)
    {
    	final int netmask = src[4] & 0xff;
    	
        return (src[0] & 0xff) + "." + (src[1] & 0xff) + "." + 
        	   (src[2] & 0xff) + "." + (src[3] & 0xff) +
        	   (netmask <= 32 ? "/" + netmask : ""); 
    }

	public static Inet4Address textToAddr(String... s) {

		byte[] res = new byte[5];

		long val;
		try {
			switch (s.length) {
			case 1:
				/*
				 * When only one part is given, the value is stored directly in
				 * the network address without any byte rearrangement.
				 */

				val = Long.parseLong(s[0]);
				if (val < 0 || val > 0xffffffffL)
					return null;
				res[0] = (byte) ((val >> 24) & 0xff);
				res[1] = (byte) (((val & 0xffffff) >> 16) & 0xff);
				res[2] = (byte) (((val & 0xffff) >> 8) & 0xff);
				res[3] = (byte) (val & 0xff);
				res[4] = (byte) (33 & 0xff);
				break;
			case 2:
				/*
				 * When a two part address is supplied, the last part is
				 * interpreted as a 24-bit quantity and placed in the right most
				 * three bytes of the network address. This makes the two part
				 * address format convenient for specifying Class A network
				 * addresses as net.host.
				 */

				val = Integer.parseInt(s[0]);
				if (val < 0 || val > 0xff)
					return null;
				res[0] = (byte) (val & 0xff);
				val = Integer.parseInt(s[1]);
				if (val < 0 || val > 0xffffff)
					return null;
				res[1] = (byte) ((val >> 16) & 0xff);
				res[2] = (byte) (((val & 0xffff) >> 8) & 0xff);
				res[3] = (byte) (val & 0xff);
				res[4] = (byte) (33 & 0xff);
				break;
			case 3:
				/*
				 * When a three part address is specified, the last part is
				 * interpreted as a 16-bit quantity and placed in the right most
				 * two bytes of the network address. This makes the three part
				 * address format convenient for specifying Class B net- work
				 * addresses as 128.net.host.
				 */
				for (int i = 0; i < 2; i++) {
					val = Integer.parseInt(s[i]);
					if (val < 0 || val > 0xff)
						return null;
					res[i] = (byte) (val & 0xff);
				}
				val = Integer.parseInt(s[2]);
				if (val < 0 || val > 0xffff)
					return null;
				res[2] = (byte) ((val >> 8) & 0xff);
				res[3] = (byte) (val & 0xff);
				res[4] = (byte) (33 & 0xff);
				break;
			case 4:
				/*
				 * When four parts are specified, each is interpreted as a byte
				 * of data and assigned, from left to right, to the four bytes
				 * of an IPv4 address.
				 */
				for (int i = 0; i < 4; i++) {
					val = Integer.parseInt(s[i]);
					if (val < 0 || val > 0xff)
						return null;
					res[i] = (byte) (val & 0xff);
				}
				res[4] = (byte) (33 & 0xff);
				break;
			case 5:
				/*
				 * When five parts are specified, each is interpreted as a byte
				 * of data and assigned, from left to right, to the four bytes
				 * of an IPv4 address plus the one byte for the netmask.
				 */
				for (int i = 0; i < 5; i++) {
					val = Integer.parseInt(s[i]);
					if (val < 0 || val > 32)
						return null;
					res[i] = (byte) (val & 0xff);
				}
				break;
			default:
				return null;
			}
		} catch (NumberFormatException e) {
			return null;
		}
		return new Inet4Address(res);
	}

}
