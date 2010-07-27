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

package com.bigdata.util.config;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Inet4Address;
import java.net.InterfaceAddress;
import java.net.MalformedURLException;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Enumeration;
import java.util.Collections;
import java.util.logging.LogRecord;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import com.sun.jini.config.Config;
import com.sun.jini.logging.Levels;

/**
 * Utility class that provides a set of static convenience methods
 * related to processing information about the current node's Network
 * Interface Card(s) (NICs) and associated IP address(es) and hostname.
 * Although useful in general, the methods in this utility class may
 * be particularly useful when employed from within a Jini configuration
 * file.
 * <p>
 * This class cannot be instantiated.
 */
public class NicUtil {

    private static final org.apache.log4j.Logger utilLogger = 
                                     LogUtil.getLog4jLogger( NicUtil.class );

    private static final java.util.logging.Logger jiniConfigLogger = 
                       java.util.logging.Logger.getLogger("net.jini.config");
    private static final java.util.logging.Level WARNING = 
                                             java.util.logging.Level.WARNING;
    private static final java.util.logging.Level INFO = 
                                              java.util.logging.Level.INFO;
    private static final java.util.logging.Level CONFIG = 
                                              java.util.logging.Level.CONFIG;

    // This class cannot be instantiated.
    private NicUtil() {
        throw new AssertionError
                   ("com.bigdata.util.NicUtil cannot be instantiated");
    }
      
    /**
     * Method that searches for and returns the network interface having
     * the specified <code>name</code>.
     *
     * @param name <code>String</code> referencing the name of the 
     *             network interface to return (for example, typical
     *             values for this parameter might be, "eth0", "eth1",
     *             "hme01", "lo", etc., depending on how the underlying
     *             platform is configured).
     *
     * @return an instance of <code>NetworkInterface</code> that represents
     *         the network interface corresponding to the given 
     *         <code>name</code>, or <code>null</code> if there is no
     *         network interface with that name value.
     *
     * @throws SocketException if there is an error in the underlying
     *         I/O subsystem and/or protocol.
     *
     * @throws NullPointerException if <code>null</code> is input for
     *         <code>name</code>.
     */
    public static NetworkInterface getNetworkInterface(String name) 
                                                      throws SocketException
    {
        NetworkInterface nic = NetworkInterface.getByName(name);
        if (nic == null) {
            // try by IP address
            InetAddress targetIp = null;
            try {
                targetIp = InetAddress.getByName(name);
                nic = NetworkInterface.getByInetAddress(targetIp);
            } catch (UnknownHostException uhe) {
                // ignore, return null
            }
        }
        return nic;
    }

    /**
     * Method that returns a <code>Map</code> in which the key component
     * of each element is one of the addresses of one of the network
     * interface cards (nics) installed on the current node, and the
     * corresponding value component is the name of the associated
     * nic to which that address is assigned.
     *
     * @return a <code>Map</code> of key-value pairs in which the key
     *         is an instance of <code>InetAddress</code> referencing
     *         the address of one of the network interface cards installed
     *         on the current node, and the corresponding value is a
     *         <code>String</code> referencing the name of the associated
     *         network interface to which the address is assigned.
     *
     * @throws SocketException if there is an error in the underlying
     *         I/O subsystem and/or protocol.
     */
    public static Map<InetAddress, String> getInetAddressMap() 
                                                      throws SocketException
    {
        Map<InetAddress, String> retMap = new HashMap<InetAddress, String>();

        //get all nics on the current node
        Enumeration<NetworkInterface> nics = 
                                    NetworkInterface.getNetworkInterfaces();

        while( nics.hasMoreElements() ) {
            NetworkInterface curNic = nics.nextElement();
            Enumeration<InetAddress> curNicAddrs = curNic.getInetAddresses();
            String curNicName = curNic.getName();
            while( curNicAddrs.hasMoreElements() ) {
                retMap.put( curNicAddrs.nextElement(), curNicName );
            }
        }
        return retMap;
    }

    /**
     * Method that searches for and returns an array whose elements
     * are all the network interface(s) that correspond to the specified
     * <code>name</code>.
     *
     * @param name <code>String</code> referencing the name to which
     *             the desired network interface(s) correspond.
     *
     * @return an array whose elements are each instances of 
     *         <code>NetworkInterface[]</code>, in which each such
     *         instance corresponds to the given <code>name</code>,
     *         or <code>null</code> if there is no network interface
     *         corresponding to that name value.
     *         
     *         Note that if the value given for the <code>name</code> 
     *         parameter is the <code>String</code> "all", then this
     *         method will return an array containing all of the
     *         network interfaces installed on the current node, 
     *         regardless of each interface's name.
     *
     * @throws SocketException if there is an error in the underlying
     *         I/O subsystem and/or protocol.
     *
     * @throws NullPointerException if <code>null</code> is input for
     *         <code>name</code>.
     */
    public static NetworkInterface[] getNetworkInterfaceArray(String name) 
                                                      throws SocketException
    {
        NetworkInterface [] nics = null;
        if (name.equals("all")) {
	    Enumeration en = NetworkInterface.getNetworkInterfaces();
	    List nicList = (en != null) ?
		Collections.list(en) : Collections.EMPTY_LIST;
            nics = (NetworkInterface[])(nicList.toArray
                                     (new NetworkInterface[nicList.size()]) );
        } else {
            nics = new NetworkInterface[1];
            nics[0] = NetworkInterface.getByName(name);
            if (nics[0] == null) {
                // try to lookup by IP address
                InetAddress targetIp = null;
                try {
                    targetIp = InetAddress.getByName(name);
                    nics[0] = NetworkInterface.getByInetAddress(targetIp);
                } catch (UnknownHostException uhe) {
                    // ignore, return null
                }
            }
        }
        return nics;
    }

    /**
     * Returns the instance of <code>InetAddress</code> that represents
     * the i-th IP address assigned to the network interface having the
     * given <code>name</code> (where i is specified by the value of the
     * <code>index</code> parameter). 
     * <p>
     * If this method fails to retrieve the desired value for the 
     * given network interface <code>name</code> -- either because of
     * a system error, or because a network interface with that name
     * doesn't exist -- then this method provides the following
     * <i>optional</i> fallback strategies, which will be executed in
     * the order documented below:
     *
     * <p><ul>
     *  <li> if the <code>host</code> parameter is non-<code>null</code>
     *  <li> return the <code>InetAddress</code> for the given
     *       <code>host</code> name (or IP address string value)
     * </ul></p>
     *
     * If the previous <code>fallback</code> strategy fails, then
     *
     * <p><ul>
     *  <li> if the <code>localHost</code> parameter is <code>true</code>
     *  <li> return the <code>InetAddress</code> for system local host
     * </ul></p>
     *
     * Thus, although this method gives priority to the network interface
     * <code>name</code>, if one wishes to force this method to return
     * the <code>InetAddress</code> for a given host name rather than for
     * a network interface, then <code>null</code> (or a name value known
     * to not exist on the system) should be input for the <code>name</code>
     * parameter. Similarly, if one wishes to force this method to return
     * the <code>InetAddress</code> of the local host, then <code>null</code>
     * should be input for both the <code>name</code> parameter and the 
     * <code>host</code> parameter.
     * <p>
     * If each of the strategies described above fail, then this method
     * returns <code>null</code>.
     *
     * @param name      <code>String</code> referencing the name of the 
     *                  network interface to query for the desired address.
     *
     * @param index     non-negative <code>int</code> value that indicates
     *                  which IP address, from the list of IP address(es)
     *                  assigned to the network interface, should be 
     *                  used when retrieving the <code>InetAddress</code> 
     *                  to return. Note that 0 is typically input for
     *                  this value.
     *
     * @param host      <code>String</code> referencing the name of the
     *                  host whose <code>InetAddress</code> should be
     *                  returned if failure occurs for the <code>name</code>
     *                  parameter.
     *
     * @param localHost if <code>true</code>, then upon failure to retrieve
     *                  a valid value for the given <code>name</code> and
     *                  the given <code>host</code> (in that order), 
     *                  attempt to return the <code>InetAddress</code> of
     *                  the local host.
     *
     * @return the instance of <code>InetAddress</code> that represents
     *         the <code>index</code>-th IP address assigned to the
     *         network interface having the given <code>name</code>,
     *         or the given <code>host</code> name, or the local host.
     *
     * @throws NullPointerException if <code>null</code> is input for
     *         both <code>name</code> and <code>host</code>, and 
     *         <code>localHost</code> is <code>false</code>.
     *
     * @throws IllegalArgumentException if the value input for 
     *         <code>index</code> is negtive.
     *
     * @throws IndexOutOfBoundsException if the value input for 
     *         <code>index</code> is out of range; that is if the value
     *         input is greater than or equal to the number of IP 
     *         address(es) assigned to the corresponding network interface.
     */
    public static InetAddress getInetAddress(String  name,
                                             int     index,
                                             String  host,
                                             boolean localHost)
    {
        // Validate input parameters 
        if( (name == null) && (host == null) && (localHost == false) ) {
            throw new NullPointerException("name cannot be null");
        }
        if(index < 0) throw new IllegalArgumentException
                                             ("index cannot be negative");
        // Primary retrieval attempt 
        NetworkInterface nic = null;
        try {
            nic = getNetworkInterface(name);
        } catch(Exception e) {/* swallow and try fallback */}
        if(nic != null) {
            List<InterfaceAddress> interfaceAddrs = 
                                             nic.getInterfaceAddresses();
            if(interfaceAddrs.size() == 0) return null;
            int inet4AddrIndex = 0;
            for(int i=0; i<interfaceAddrs.size();i++) {
                InetAddress inetAddr = (interfaceAddrs.get(i)).getAddress();
                if(inetAddr instanceof Inet4Address) {
                    if(index == inet4AddrIndex) {
                        Inet4Address inet4Addr = (Inet4Address)inetAddr;
                        String hostAddr = inet4Addr.getHostAddress();
                        String hostName = inet4Addr.getCanonicalHostName();
                        jiniConfigLogger.log(CONFIG, 
                                             "Inet4: address = "+hostAddr
                                             +", name = "+hostName);
                        utilLogger.log(Level.TRACE, 
                                       "Inet4: address = "+hostAddr
                                       +", name = "+hostName);
                        return inetAddr;
                    } else {
                        inet4AddrIndex = inet4AddrIndex+1;//next index
                    }
                }
            }

        }

        InetAddress fallback = null;

        // Nic-based retrieval failed. Try host name? 
        if(host != null) {
            try {
		fallback = InetAddress.getByName(host);
            } catch(Exception e) {/* swallow and try fallback */}
            if(fallback != null) {
                jiniConfigLogger.log(CONFIG, "fallback host = "+fallback);
                utilLogger.log(Level.TRACE, "fallback host = "+fallback);
                return fallback;
            }
        }

        // Host-based retrieval failed. Try local host? 
        if(localHost) {
            try {
                fallback = InetAddress.getLocalHost();
            } catch(Exception e) {/* swallow and return null */}
            jiniConfigLogger.log(CONFIG, "fallback local host = "+fallback);
            utilLogger.log(Level.TRACE, "fallback local host = "+fallback);
        }

        return fallback;

    }

    /**
     * Method that returns the <i>Media Access Control (MAC)</i> address
     * assigned to the network interface having the given <code>name</code>;
     * returning the address as a <code>String</code> in a human-readable
     * format that consists of six groups of two hexadecimal digits,
     * separated by colons (:); e.g., <code>01:23:45:67:89:ab</code>.
     * <p>
     * If this method fails to retrieve the desired MAC address for the given
     * network interface <code>name</code> -- either because of a system
     * error, or because a network interface with the given <code>name</code>
     * does not exist -- then <code>null</code> is returned.
     *
     * @param name <code>String</code> referencing the name of the 
     *             network interface whose MAC address should be 
     *             returned.
     *
     * @return a <code>String</code>, in human-readable format, whose value
     *         is constructed from the MAC address assigned to the network
     *         interface having the given <code>name</code>; or
     *         <code>null</code> if the MAC address of the desired network
     *         interface cannot be retrieved.
     *
     * @throws SocketException if there is an error in the underlying
     *         I/O subsystem and/or protocol.
     *
     * @throws NullPointerException if <code>null</code> is input for
     *         <code>name</code>.
     */
    public static String getMacAddress(String name) throws SocketException {
        String macAddr = null;
        NetworkInterface nic = NicUtil.getNetworkInterface(name);
        byte[] hwAddr = nic.getHardwareAddress();
        if( (hwAddr != null) && (hwAddr.length > 0) ) {
            StringBuffer strBuf = new StringBuffer();
            for(int i=0; i<hwAddr.length; i++) {
                String subStr = String.format("%02X", hwAddr[i]);
                if(i == 0) {
                    strBuf.append(subStr);
                } else {
                    strBuf.append(":"+subStr);
                }
            }
            macAddr = strBuf.toString();
        }
        return macAddr;
    }

    /**
     * Three-argument version of <code>getInetAddress</code> that retrieves
     * the desired interface name from the given <code>Configuration</code>
     * parameter.
     */
    public static InetAddress getInetAddress(Configuration config,
                                             String        componentName,
                                             String        nicNameEntry)
    {
        String nicName = "NoNetworkInterfaceName";
        try {
            nicName = (String)Config.getNonNullEntry(config,
                                                     componentName,
                                                     nicNameEntry,
                                                     String.class,
                                                     "eth0");
        } catch(ConfigurationException e) {
            jiniConfigLogger.log(WARNING, e
                                 +" - [componentName="+componentName
                                 +", nicNameEntry="+nicNameEntry+"]");
            utilLogger.log(Level.WARN, e
                           +" - [componentName="+componentName
                           +", nicNameEntry="+nicNameEntry+"]");
            e.printStackTrace();
            return null;
        }
        return ( getInetAddress(nicName, 0, null, false) );
    }

    // What follows are a number of versions of the getIpAddress method
    // provided for convenience.

    /**
     * Returns the <code>String</code> value of the 0-th IP address assigned
     * to the network interface or host having the given <code>name</code>,
     * or <code>null</code> if that IP address cannot be retrieved.
     */
    public static String getIpAddress(String name) {
        InetAddress inetAddr = getInetAddress(name, 0, name, false);
        if(inetAddr != null) return inetAddr.getHostAddress();
        return null;
    }

    public static String getIpAddress(String name, int index) {
        InetAddress inetAddr = getInetAddress(name, index, name, false);
        if(inetAddr != null) return inetAddr.getHostAddress();
        return null;
    }

    public static String getIpAddress(String name, String host) {
        InetAddress inetAddr = getInetAddress(name, 0, host, false);
        if(inetAddr != null) return inetAddr.getHostAddress();
        return null;
    }

    public static String getIpAddress(String name, int index, String host) {
        InetAddress inetAddr = getInetAddress(name, index, host, false);
        if(inetAddr != null) return inetAddr.getHostAddress();
        return null;
    }

    public static String getIpAddress(String  name,
                                      int     index,
                                      String  host,
                                      boolean localHost)
    {
        InetAddress inetAddr = getInetAddress(name, index, host, localHost);
        if(inetAddr != null) return inetAddr.getHostAddress();
        return null;
    }

    public static String getIpAddress(String  name, boolean localHost) {
        InetAddress inetAddr = getInetAddress(name, 0, name, localHost);
        if(inetAddr != null) return inetAddr.getHostAddress();
        return null;
    }

    public static String getIpAddress(String  name, 
                                      int     index, 
                                      boolean localHost)
    {
        InetAddress inetAddr = getInetAddress(name, index, name, localHost);
        if(inetAddr != null) return inetAddr.getHostAddress();
        return null;
    }

    public static String getIpAddressByHost(String host) {
        InetAddress inetAddr = getInetAddress(null, 0, host, false);
        if(inetAddr != null) return inetAddr.getHostAddress();
        return null;
    }

    public static String getIpAddressByHost(String host, boolean localHost) {
        InetAddress inetAddr = getInetAddress(null, 0, host, localHost);
        if(inetAddr != null) return inetAddr.getHostAddress();
        return null;
    }

    public static String getIpAddressByLocalHost() {
        InetAddress inetAddr = getInetAddress(null, 0, null, true);
        if(inetAddr != null) return inetAddr.getHostAddress();
        return null;
    }

    /**
     * Special-purpose convenience method that returns a
     * <code>String</code> value representing the ip address of 
     * the current node.
     * <p>
     * If a non-<code>null</code> value is input for the
     * <code>systemPropertyName</code> parameter, then this
     * method first determines if a system property with
     * name equivalent to the given value has been set and,
     * if it has, returns the ip address of the nic whose name
     * is equivalent to that system property value; or
     * <code>null</code> if there is no nic with the desired
     * name installed on the node.
     * <p>
     * If there is no system property whose name is the value
     * of the <code>systemPropertyName</code> parameter, and
     * if the value "default" is input for the 
     * <code>defaultNic</code> parameter, then this method
     * will return the IPV4 based address of the first reachable
     * nic that can be found on the node; otherwise, if a
     * non-<code>null</code> value not equal to "default" is
     * input for the the <code>defaultNic</code> parameter, 
     * then this method returns the ip address of the nic 
     * corresponding to that given name; or <code>null</code>
     * if there is no such nic name installed on the node.
     * <p>
     * If, on the other hand, <code>null</code> is input for
     * the <code>systemPropertyName</code> parameter, then 
     * this method will attempt to find the desired ip address
     * using only the value of the <code>defaultNic</code>,
     * and applying the same search criteria as described
     * above.
     * <p>
     * Note that in all cases, if <code>true</code> is input
     * for the <code>loopOk</code> parameter, then upon failing
     * to find a valid ip address using the specified search
     * mechanism, this method will return the <i>loop back</i>
     * address; otherwise, <code>null</code> is returned.
     * <p>
     * This method can be called from within a configuration
     * as well as from within program control.
     *
     * @param systemPropertyName <code>String</code> value containing
     *                           the name of a system property whose
     *                           value is the network interface name
     *                           whose ip address should be returned.
     *                           May be <code>null</code>.
     *
     * @param defaultNic         <code>String</code> value containing
     *                           the name of the network interface
     *                           whose ip address should be returned
     *                           if <code>null</code> is input for the
     *                           <code>systemPropertyName</code> parameter,
     *                           or if there is no system property with
     *                           name equivalent the value of the
     *                           <code>systemPropertyName</code> parameter.
     *
     * @param loopbackOk         if <code>true</code>, then return the
     *                           <i>loop back</i> address upon failure
     *                           to find a valid ip address using the 
     *                           search criteria specified through the
     *                           <code>systemPropertyName</code> and
     *                           <code>defaultNic</code> parameters.
     *
     * @return a <code>String</code> representing an ip address associated
     *         with the current node; where the value that is returned is
     *         determined according to the criteria described above.
     */
    public static String getIpAddress(String  systemPropertyName,
                                      String  defaultNic,
                                      boolean loopbackOk)
                             throws SocketException, IOException
    {
        if(systemPropertyName != null) {//system property takes precedence
            String nicName = System.getProperty(systemPropertyName);
            boolean propSet = true;
            if(nicName == null) {
                propSet = false;
            } else {
                // handle ant script case where the system property
                // may not have been set on the command line, but
                // was still set to "${<systemPropertyName>}" using
                // ant <sysproperty> tag
                String rawProp = "${" + systemPropertyName + "}";
                if( rawProp.equals(nicName) ) propSet = false;
            }
            if(propSet) {
                return getIpAddress(nicName, 0, loopbackOk);
            } else {//system property not set, try default and/or fallback
                if(defaultNic != null) {
                    if( defaultNic.equals("default") ) {
                        return getDefaultIpv4Address(loopbackOk);
                    } else {
                        return getIpAddress(defaultNic, 0, loopbackOk);
                    }
                } else {
                    return null;
                }
            }
        } else {//no system property name provided, try default
            if(defaultNic != null) {
                if( defaultNic.equals("default") ) {
                    return getDefaultIpv4Address(loopbackOk);
                } else {
                    return getIpAddress(defaultNic, 0, loopbackOk);
                }
            } else {
                return getIpAddress(null, loopbackOk);
            }
        }
    }

    /**
     * Examines each address associated with each network interface
     * card (nic) installed on the current node, and returns the
     * <code>String</code> value of the first such address that is
     * determined to be both <i>reachable</i> and an address type
     * that represents an <i>IPv4</i> address.
     *
     * This method will always first examine addresses that are
     * <i>not</i> the <i>loopback</i> address (<i>local host</i>);
     * returning a loopback adddress only if <code>true</code>
     * is input for the <code>loopbackOk</code> parameter, and
     * none of the non-loopback addresses satisfy this method's
     * search criteria.
     *
     * If this method fails to find any address that satisfies the
     * above criteria, then this method returns <code>null</code>.
     *
     * @param loopbackOk if <code>true</code>, then upon failure
     *                   find an non-<i>loopback</i> address that
     *                   satisfies this method's search criteria
     *                   (an IPv4 type address and reachable), the
     *                   first loopback address that is found to be
     *                   reachable is returned.
     *
     *                   If <code>false</code> is input for this
     *                   parameter, then this method will examine
     *                   only those addresses that do <i>not</i>
     *                   correspond to the corresponding nic's
     *                   loopback address.
     *
     * @return a <code>String</code> value representing the first
     *         network interface address installed on the current
     *         node that is determined to be both <i>reachable</i>
     *         and an IPv4 type address; where the return value
     *         corresponds to a <i>loopback</i> address only if 
     *         <code>true</code> is input for the <code>loopbackOk</code>
     *         parameter, and no non-loopback address satisfying
     *         the desired criteria can be found. If this method
     *         fails to find any address that satisfies the desired
     *         criteria, then <code>null</code> is returned.
     *
     * @throws SocketException if there is an error in the underlying
     *         I/O subsystem and/or protocol while retrieving the
     *         the network interfaces currently installed on the
     *         node.
     *
     * @throws IOException if a network error occurs while determining
     *         if a candidate return address is <i>reachable</i>.
     */
    public static String getDefaultIpv4Address(boolean loopbackOk)
                             throws SocketException, IOException
    {
        //get all nics on the current node
        Enumeration<NetworkInterface> nics = 
            NetworkInterface.getNetworkInterfaces();
        while( nics.hasMoreElements() ) {
            NetworkInterface curNic = nics.nextElement();
            List<InterfaceAddress> interfaceAddrs = 
                                       curNic.getInterfaceAddresses();
            for(InterfaceAddress interfaceAddr : interfaceAddrs) {
                InetAddress inetAddr = interfaceAddr.getAddress();
                boolean isIpv4 = inetAddr instanceof Inet4Address;
                boolean isLoopbackAddress = inetAddr.isLoopbackAddress();
                if(isIpv4) {
                    if(isLoopbackAddress) continue;
                    boolean isReachable = inetAddr.isReachable(3*1000);
                    Inet4Address inet4Addr = (Inet4Address)inetAddr;
                    String retVal = inet4Addr.getHostAddress();

                    jiniConfigLogger.log
                        (CONFIG, "default IPv4 address: "+retVal);
                    utilLogger.log
                        (Level.TRACE, "default IPv4 address: "+retVal);
                    return retVal;
                }
            }
        }

        if(!loopbackOk) return null;

        nics = NetworkInterface.getNetworkInterfaces();
        while( nics.hasMoreElements() ) {
            NetworkInterface curNic = nics.nextElement();
            List<InterfaceAddress> interfaceAddrs = 
                                       curNic.getInterfaceAddresses();
            for(InterfaceAddress interfaceAddr : interfaceAddrs) {
                InetAddress inetAddr = interfaceAddr.getAddress();
                boolean isIpv4 = inetAddr instanceof Inet4Address;
                boolean isLoopbackAddress = inetAddr.isLoopbackAddress();
                if(isIpv4) {
                    if(!isLoopbackAddress) continue;
                    boolean isReachable = inetAddr.isReachable(3*1000);
                    Inet4Address inet4Addr = (Inet4Address)inetAddr;
                    String retVal = inet4Addr.getHostAddress();

                    jiniConfigLogger.log
                        (CONFIG, "default IPv4 address: "+retVal);
                    utilLogger.log
                        (Level.TRACE, "default IPv4 address: "+retVal);
                    return retVal;
                }
            }
        }
        return null;
    }

    public static String getDefaultIpv4Address()
                             throws SocketException, IOException
    {
        return getDefaultIpv4Address(false);//localhost NOT ok
    }

    /**
     * Intended for use by scripts.
     */
    public static void main(String[] args) {
        String ipAddress = "NIC_DOES_NOT_EXIST";
        try {
            if(args.length == 0) {
                ipAddress = getIpAddressByLocalHost();
            } else {
                if( args[0].equals("getIpAddressByLocalHost") ) {
                    ipAddress = getIpAddressByLocalHost();
                } else if( args[0].equals("getIpAddress") ) {
                    if(args.length == 2) {
                        String tmpIpAddress = NicUtil.getIpAddress(args[1]);
                        if(tmpIpAddress != null) ipAddress = tmpIpAddress;
                    } else if(args.length == 3) {
                        String tmpIpAddress = 
                               NicUtil.getIpAddress(args[1],0,args[2],false);
                        if(tmpIpAddress != null) ipAddress = tmpIpAddress;
                    }
                } else {
                    ipAddress = "NIC_UTIL_FAILURE";
                    System.out.println("NicUtil Failure: unexpected number of "
                                       +"arguments ("+args.length+")");
                }
            }
        } catch(Throwable t) {
            ipAddress = "NIC_UTIL_FAILURE";
            t.printStackTrace();
        }
        System.out.println(ipAddress);
    }

}