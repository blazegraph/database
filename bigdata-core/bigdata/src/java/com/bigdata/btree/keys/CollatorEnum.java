package com.bigdata.btree.keys;

/**
 * Type-safe enumeration of collators that may be configured.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public enum CollatorEnum {

    /**
     * The JDK bundles support for generating Unicode sort keys, but that
     * support does NOT include compressed sort keys and embeds <code>nul</code>
     * bytes into its Unicode sort keys.
     */
    JDK,
    
    /**
     * ICU is the basis for the Unicode support in the JDK and also supports
     * compressed sort keys, which can be a big savings in an index.
     */
    ICU,

//    /**
//     * A JNI plugin for ICU (native code for faster generation of sort keys).
//     * 
//     * @deprecated ICU4JNI is no longer being developed. The ICU project has
//     *             provided high performance pure Java code for all aspects of
//     *             ICU which were historically optimized only in ICU4C.
//     */
//    ICU4JNI,

    /**
     * A configuration option to force the interpretation of Unicode text as
     * ASCII (only the low byte is considered). This option can be useful
     * when you know that your data is not actually Unicode and offers a
     * substantial performance benefit in such cases.
     */
    ASCII;
    
}