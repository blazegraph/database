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

import com.bigdata.attr.ServiceInfo;

import com.sun.jini.config.Config;
import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.core.discovery.LookupLocator;
import net.jini.discovery.DiscoveryManagement;
import net.jini.discovery.DiscoveryGroupManagement;
import net.jini.discovery.DiscoveryLocatorManagement;
import net.jini.discovery.LookupDiscoveryManager;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.SocketException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * Utility class containing a number of convenient methods that encapsulate
 * common functions related to configuration and deployment of the entity.
 * The methods of this class are <code>static</code> so that they can be
 * invoked from within a Jini configuration.
 */
public class ConfigDeployUtil {

    private static Properties deploymentProps = null;

    private static final String DEFAULT = ".default";
    private static final String STRINGVALS = ".stringvals";
    private static final String MAX = ".max";
    private static final String MIN = ".min";
    private static final String DESCRIPTION = ".description";
    private static final String TYPE = ".type";

    public static String getString(String parameter) 
                             throws ConfigurationException
    {
        String value = get(parameter);
        validateString(parameter, value);
        return value;
    }

    public static String[] getStringArray(String parameter)
                               throws ConfigurationException
    {
        String[] value;
        value = validateStringArray(parameter, get(parameter));
        return value;
    }

    public static int getInt(String parameter) throws ConfigurationException {
        int value;
        value = validateInt(parameter, get(parameter));
        return value;
    }

    public static long getLong(String parameter) throws ConfigurationException
    {
        long value;
        value = validateLong(parameter, get(parameter));
        return value;
    }

    public static boolean getBoolean(String parameter) 
                              throws ConfigurationException {
        boolean boolValue = false;
        String value;
        try {
            value = get(parameter);
        } catch (Exception ex) {
            throw new ConfigurationException("parameter value ["+parameter+"] "
                                             +"neither 'true' nor 'false'");
        }
        if( value != null && value.equalsIgnoreCase("true")
           || value.equalsIgnoreCase("false") )
        {
            boolValue = Boolean.parseBoolean(value);
        } else {
            throw new ConfigurationException("parameter value ["+parameter+"] "
                                             +"neither 'true' nor 'false'");
        }
        return boolValue;
    }

    public static String getDescription(String parameter)
                             throws ConfigurationException
    {
        String value;
        if(deploymentProps == null) {
            deploymentProps = new Properties();
            loadDeployProps(deploymentProps);
        }
        value = deploymentProps.getProperty(parameter + DESCRIPTION);
        return value;
    }

    public static String getType(String parameter)
                             throws ConfigurationException
    {
        String value;
        if (deploymentProps == null) {
            deploymentProps = new Properties();
            loadDeployProps(deploymentProps);
        }
        value = deploymentProps.getProperty(parameter + TYPE);
        return value;
    }

    public static String getDefault(String parameter) 
                             throws ConfigurationException
    {
        String value;
        if (deploymentProps == null) {
            deploymentProps = new Properties();
            loadDeployProps(deploymentProps);
        }
        value = deploymentProps.getProperty(parameter + DEFAULT);
        if (value == null) {
            throw new ConfigurationException
                          ("deployment parameter not found ["+parameter+"]");
        }
        return value;
    }

    /**
     * Returns a <code>String</code> array whose elments represent the
     * lookup service groups to discover. If the system property named
     * "federation.name" is set then that value be used; otherwise,
     * the deployment properties files will be consulted.
     *
     * @throws ConfigurationException if the groups cannot be determined.
     */
    public static String[] getGroupsToDiscover() throws ConfigurationException
    {
        String fedNameStr = System.getProperty("federation.name");
        if(fedNameStr == null) {
            fedNameStr = getString("federation.name");
        }
        return fedNameStr.split(",");
    }

    /**
     * Returns an array of <code>LookupLocator</code> instances that can
     * each be used to discover a specific lookup service.
     *
     * @throws ConfigurationException if the locators cannot be determined.
     */
    public static LookupLocator[] getLocatorsToDiscover()
                                      throws ConfigurationException
    {
        return new LookupLocator[]{};
    }

    /** 
     * Returns an instance of the <code>ServiceInfo</code> attribute class,
     * initialized to the values specified in the deployment properties
     * files.
     */
    public static ServiceInfo initServiceInfo(UUID source, String serviceName)
                                throws SocketException, ConfigurationException
    {
        ServiceInfo serviceInfo = new ServiceInfo();
        serviceInfo.source = source;
        serviceInfo.serviceName = serviceName;

        serviceInfo.inetAddresses = NicUtil.getInetAddressMap();

        // Get the common token that all services running on the same
        // node agree on. Use the MAC address or IP address as default token
        String nodeNicName = getString("node.serviceNetwork");
        String nodeIp = NicUtil.getIpAddress(nodeNicName, false);
        serviceInfo.nodeToken = NicUtil.getMacAddress(nodeNicName);
        if(serviceInfo.nodeToken == null) serviceInfo.nodeToken = nodeIp;

        serviceInfo.nodeId = null;//not set until a node service exists
        serviceInfo.nodeName = getString("node.name");

        serviceInfo.uNumber = getInt("node.uNumber");

        serviceInfo.rack   = getString("node.rack");
        serviceInfo.cage   = getString("node.cage");
        serviceInfo.zone   = getString("node.zone");
        serviceInfo.site   = getString("node.site");
        serviceInfo.region = getString("node.region");
        serviceInfo.geo    = getString("node.geo");

        return serviceInfo;
    }


    private static String get(String parameter) throws ConfigurationException {
        String value;
        if (deploymentProps == null) {
            deploymentProps = new Properties();
            loadDeployProps(deploymentProps);
        }
        value = deploymentProps.getProperty(parameter);
        if (value == null) value = getDefault(parameter);
        return value;
    }

    private static void validateString(String parameter, String value) 
                            throws ConfigurationException
    {
        String validValuesStr = 
            (String) deploymentProps.get(parameter + STRINGVALS);

        if (validValuesStr != null) {
            String[] validValues = validValuesStr.split(",");
            if (!Arrays.asList(validValues).contains(value)) {
                throw new ConfigurationException
                              ("invalid string parameter ["+parameter+"] in "
                               +"list ["+validValuesStr+"]");
            }
        }
        return;
    }

    private static String[] validateStringArray(String parameter, String value)
                                throws ConfigurationException
    {
        String validValuesStr = 
            (String)(deploymentProps.get(parameter + STRINGVALS));
        String[] values = value.split(",");

        if (validValuesStr != null) {
            String[] validValues = validValuesStr.split(",");
            List validValuesList = Arrays.asList(validValues);
            for (int i=0; i<values.length; i++) {
                if (!validValuesList.contains(values[i])) {
                    throw new ConfigurationException
                              ("invalid string parameter ["+parameter+"] in "
                               +"list "+validValuesList);
                }
            }
        }
        return values;
    }

    private static int validateInt(String parameter, String strvalue)
                           throws ConfigurationException
    {
        String maxString = (String)(deploymentProps.get(parameter + MAX));
        String minString = (String)(deploymentProps.get(parameter + MIN));

        int value = str2int(strvalue);

        if (maxString != null) {
            int max = Integer.parseInt(maxString);
            if (value > max) {
                throw new ConfigurationException("parameter ["+parameter+"] "
                                                 +"exceeds maximum ["+max+"]");
            }
        }
        return value;
    }

    private static long validateLong(String parameter, String strvalue) 
                            throws ConfigurationException
    {
        String maxString = (String)(deploymentProps.get(parameter + MAX));
        String minString = (String)(deploymentProps.get(parameter + MIN));

        long value = str2long(strvalue);

        if (maxString != null) {
            long max = Long.parseLong(maxString);
            if (value > max) {
                throw new ConfigurationException("parameter ["+parameter+"] "
                                                 +"exceeds maximum ["+max+"]");
            }
        }
        if (minString != null) {
            long min = Long.parseLong(minString);
            if (value < min) {
                throw new ConfigurationException("parameter ["+parameter+"] "
                                                 +"is less than manimum "
                                                 +"["+min+"]");
            }
        }
        return value;
    }


    private static String getPropertiesPath() {
        String rootPath = "/opt/bigdata";//real installation
        String appHome = System.getProperty("appHome");//pstart
        String appDotHome = System.getProperty("app.home");//build.xml

        if(appHome != null) {
            rootPath = appHome;
        } else if(appDotHome != null) {
            rootPath = appDotHome + File.separator 
                               + "dist" + File.separator + "bigdata";
        }
        String relPath = "var" + File.separator + "config" 
                               + File.separator + "deploy";
        String retPath = rootPath + File.separator + relPath;
        //eclipse
        if( !(new File(retPath)).exists() ) {
            String tmpPath = "bigdata-jini" + File.separator + "src"
                                            + File.separator + "java"
                                            + File.separator + "com"
                                            + File.separator + "bigdata"
                                            + File.separator + "util"
                                            + File.separator + "config";
            retPath = (new File(tmpPath)).getAbsolutePath();
        }
        return retPath;
    }

    private static void loadDeployProps(Properties deployProps) {
        loadDefaultProps(deployProps);
        loadOverrideProps(deployProps);
    }

    private static void loadDefaultProps(Properties deployProps) {
        FileInputStream fis = null;
        try {
            String flnm = getPropertiesPath() 
                          + File.separator + "default-deploy.properties";
            fis = new FileInputStream(flnm);
            deployProps.load(fis);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException ioex) { /* swallow */ }
            }
        }
    }

    private static void loadOverrideProps(Properties deployProps) {
        FileInputStream fis = null;
        try {
            String flnm = getPropertiesPath() 
                          + File.separator + "deploy.properties";
            fis = new FileInputStream(new File(flnm));
            deployProps.load(fis);
        } catch (Exception ex) {
            // using all defaults
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException ioex) { /* swallow */ }
            }
        }
    }


    private static int str2int(String argx) {
        long l;

        if( argx.trim().equals(Integer.MAX_VALUE) ) return Integer.MAX_VALUE;
        if( argx.trim().equals(Integer.MIN_VALUE) ) return Integer.MIN_VALUE;

        l = str2long(argx);
        if (l < Integer.MAX_VALUE && l > Integer.MIN_VALUE) {
            return (int) l;
        } else {
            throw new NumberFormatException("Invalid number:"+argx
                                            +"  --number out of range");
        }
    }

    private static long str2long(String argx) {

        int minDigitNumBetwnComma = 3;

        String arg = argx.trim();
        arg = arg.replaceAll("\"", ""); // strip all quotes
        int sz = arg.length();

        if( arg.equals("Long.MAX_VALUE") ) return Long.MAX_VALUE;

        if( arg.equals("Long.MIN_VALUE") ) return Long.MIN_VALUE;

        int asterPos = -1;
        String arg1 = null;
        String arg2 = null;
        if( (asterPos = arg.indexOf("*")) != -1) {
            int dotPos = -1;
            arg1 = arg.substring(0, asterPos).trim();
            int denom1 = 1;
            if( (dotPos = arg1.indexOf(".")) != -1) {
                StringBuffer tmpBuf = new StringBuffer("1");
                int hitNumber = 0;
                for (int i = dotPos + 1; i < (arg1.length() - dotPos); i++) {
                    if( Character.isDigit(arg1.charAt(i)) ) {
                        tmpBuf.append("0");
                    } else {
                        break;
                    }
                }
                denom1 = Integer.valueOf(tmpBuf.toString());
                arg1 = arg1.substring(0, dotPos) + arg1.substring(dotPos + 1);
            }

            arg2 = arg.substring(asterPos + 1).trim();
            int denom2 = 1;
            if( (dotPos = arg2.indexOf(".")) != -1) {
                StringBuffer tmpBuf = new StringBuffer("1");
                for(int i = dotPos + 1; i <= (arg2.length() - dotPos); i++) {
                    tmpBuf.append("0");
                }

                denom2 = Integer.valueOf(tmpBuf.toString());
                arg2 = arg2.substring(0, dotPos) + arg2.substring(dotPos + 1);
            }

            long numerator = str2long(arg1) * str2long(arg2);
            long denom = (denom1 * denom2);

            if (numerator % denom != 0) {
                throw new NumberFormatException(" Bad value passed in:" +
                                                ((double) (numerator) /
                                                 denom) +
                                                ", expecting a long");
            }
            return (numerator / denom);
        }

        char unit = arg.charAt(sz - 1);

        String valScalar = arg.substring(0, (sz - 1)).trim();

        long factor = 0l;

        switch (Character.toUpperCase(unit)) {

            case 'G':
                factor = 1000000000l;
                break;
            case 'M':
                factor = 1000000l;
                break;
            case 'K':
                factor = 1000l;
                break;
            case 'B':
                char unitPre = arg.charAt(sz - 2);
                if (Character.isDigit(unitPre)) {
                    factor = -1l;
                } else {
                    factor =
                        (Character.toUpperCase(unitPre) ==
                         'G' ? 1000000000l : (Character.toUpperCase(unitPre) ==
                                          'M' ? 1000000l : (Character.
                                                            toUpperCase
                                                            (unitPre) ==
                                                            'K' ? 1000l :
                                                            -1l)));
                    valScalar = arg.substring(0, (sz - 2)).trim();
                }
                break;

            default:
                if (Character.isDigit(unit)) {
                    factor = 1l;
                    valScalar = arg;
                }
        }
        if (factor == -1l) {
            throw new NumberFormatException("Invalid number:" + arg);
        }

        int comaPos = -1;
        if( (comaPos = valScalar.indexOf(',')) != -1) {
            if(valScalar.indexOf('.') != -1) {
                throw new NumberFormatException("Invalid number:"+arg
                                                +" both \",\" and decimal "
                                                +"point are not supported");
            }
            if( comaPos != 0 && comaPos != (valScalar.length() - 1) ) {
                String[]spltByComa = valScalar.split(",");
                valScalar = "";
                for (int i = spltByComa.length - 1; i >= 0; i--) {
                    if(i > 0 && spltByComa[i].length() < minDigitNumBetwnComma)
                    {
                        throw new NumberFormatException("Invalid number:"+arg
                                                        +"  unexpected comma "
                                                        +"format");
                    }
                    valScalar = spltByComa[i] + valScalar;
                }
            } else {
                throw new NumberFormatException("Invalid number:\"" +arg
                                                +"\" -unexpected comma in "
                                                +"position: "+comaPos);
            }
        }

        int decimalPos = -1;
        String valMultiplByFactor = null;
        int numZero = 0;
        try {
            if( (decimalPos = valScalar.indexOf('.')) != -1) {
                if (decimalPos != valScalar.lastIndexOf('.')) {
                    throw new NumberFormatException("Invalid number:"
                                                    +valScalar
                                                    +"  --invalid decimal "
                                                    +"number, bad value");
                }

                String facStr = String.valueOf(factor);
                int numZeroFactor = facStr.length() - 1;
                int numDigitsAfterDecimal =
                    valScalar.length() - decimalPos - 1;
                int countZero = 0;
                for(int i = valScalar.length() - 1; i > decimalPos; i--) {

                    if (valScalar.charAt(i) != '0')
                        break;
                    --numDigitsAfterDecimal;
                    countZero++;
                }
                numZero = numZeroFactor - numDigitsAfterDecimal;
                if (numZero == numDigitsAfterDecimal) {
                    numZero = 0;
                }
                if(numZero < 0) {
                    throw new NumberFormatException("Invalid number:"
                                                    +valScalar
                                                    +"  --invalid decimal "
                                                    +"number, numzero="
                                                    + numZero);
                }

                if(numZero >= 0) {
                    StringBuffer tmpStrNum =
                        new StringBuffer(20).
                        append(valScalar.substring(0, decimalPos)).
                        append(valScalar.substring(decimalPos + 1,
                                                   decimalPos + 1 +
                                                   numDigitsAfterDecimal));
                    for(int i=0; i<numZero; i++) {
                        tmpStrNum.append('0');
                    }
                    valMultiplByFactor = tmpStrNum.toString();
                }

            }
        } catch(NumberFormatException nfe) {
            throw new NumberFormatException("Invalid number:" +valScalar
                                            +"  --invalid decimal number, "
                                            +"numZero="+numZero);
        }

        long ret = -1l;

        Long ll = ((decimalPos != -1) ? Long.valueOf(valMultiplByFactor)
                   : (Long.valueOf(valScalar) * factor));
        if( (ret = Long.valueOf(ll)) >= Long.MAX_VALUE
            || ret <= Long.MIN_VALUE)
        {
            throw new NumberFormatException("Invalid number:"+arg
                                            +"  --absolute value of number "
                                            +"too big");
        }
        return ret;
    }
}
