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
 * Created on Jul 3, 2008
 */

package com.bigdata.btree.keys;

import java.io.Serializable;
import java.util.Locale;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.bigdata.btree.keys.KeyBuilder.Options;


/**
 * Default factory for Unicode {@link IKeyBuilder}s. This does NOT generate
 * thread-local instances. The factory serializes all properties that were
 * required to generate a configuration so that the same configuration may be
 * materialized on another JVM or host by de-serializing an instance of this
 * factory.
 * 
 * @see ThreadLocalKeyBuilderFactory
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DefaultKeyBuilderFactory implements IKeyBuilderFactory, Serializable {

    private static final transient Logger log = Logger.getLogger(DefaultKeyBuilderFactory.class);
    
    /**
     * 
     */
    private static final long serialVersionUID = -3285057306742134508L;

//    private final transient boolean icu_avail;
    
    /**
     * The initial buffer capacity (grows as needed).
     */
    private final int initialCapacity;

    /**
     * The selected collator.
     */
    private final CollatorEnum collator;

    /**
     * The selected {@link Locale}.
     */
    private final Locale locale;

    /**
     * The selected collator strength (MAY be <code>null</code>, which means
     * no override).
     */
    private final Object strength;

    /**
     * The selected decomposition mode (MAY be <code>null</code>, which means
     * no override).
     */
    private final DecompositionEnum decompositionMode;

    /**
     * The initial buffer capacity (the actual capacity grows as needed at
     * runtime).
     */
    public final int getInitialCapacity() {
        
        return initialCapacity;
        
    }

    /**
     * The selected collator.
     */
    public final CollatorEnum getCollator() {
        
        return collator;
        
    }

    /**
     * The selected {@link Locale}.
     */
    public final Locale getLocale() {
        
        return locale;
        
    }

    /**
     * The selected collator strength.
     * 
     * @return Either a {@link StrengthEnum}, an {@link Integer}, or
     *         <code>null</code> (which means no override).
     */
    public final Object getStrength() {
        
        return strength;
        
    }

    /**
     * The selected decomposition mode (MAY be <code>null</code>, which means
     * no override).
     */
    public final DecompositionEnum getDecompositionMode() {
        
        return decompositionMode;
        
    }

    /**
     * Representation includes all aspects of the {@link Serializable} state.
     */
    @Override
    public String toString() {
        
        StringBuilder sb = new StringBuilder(getClass().getName());
        
        sb.append("{ initialCapacity=" + initialCapacity);
        
        sb.append(", collator=" + collator);

        sb.append(", locale=" + locale);

        sb.append(", strength=" + strength);

        sb.append(", decomposition=" + decompositionMode);
        
        sb.append("}");
        
        return sb.toString();
        
    }
    
    /**
     * Return the property if found in <i>properties</i>. If <i>properties</i>
     * is <code>null</code> or if the value is not found in <i>properties</i>,
     * then return the property if found using
     * {@link System#getProperty(String)}.
     * 
     * @param properties
     *            The properties.
     * @param key
     *            The key.
     * 
     * @return The value -or- <code>null</code> if no value was found.
     */
    static private String getProperty(final Properties properties,
            final String key) {

        return getProperty(properties, key, null);

    }

    /**
     * Return the property if found in <i>properties</i>. If <i>properties</i>
     * is <code>null</code> or if the value is not found in <i>properties</i>,
     * then return the property if found using
     * {@link System#getProperty(String)}.
     * 
     * @param properties
     *            The properties.
     * @param key
     *            The name of the desired property.
     * @param def
     *            The default (MAY be <code>null</code>).
     * 
     * @return The value -or- <i>def</i> if no value was found.
     */
    static private String getProperty(final Properties properties,
            final String key, final String def) {

        String val = null;
        
        if (properties != null) {

            val = properties.getProperty(key);//, def);

        }

        if (val == null) {

            val = System.getProperty(key, def);

        }

        if(log.isDebugEnabled()) {
            
            log.debug(key + "=" + val);
            
        }
        
        return val; 

    }
    
    /**
     * Create a factory for {@link IKeyBuilder} instances configured according
     * to the specified <i>properties</i>. Any properties NOT explicitly given
     * will be defaulted from {@link System#getProperties()}. The pre-defined
     * properties {@link Options#USER_LANGUAGE}, {@link Options#USER_COUNTRY},
     * and {@link Options#USER_VARIANT} MAY be overridden. The factory will
     * support Unicode unless {@link CollatorEnum#ASCII} is explicitly specified
     * for the {@link Options#COLLATOR} property.
     * 
     * @param properties
     *            The properties to be used (optional). When <code>null</code>
     *            the {@link System#getProperties() System properties} are used.
     * 
     * @see Options
     * 
     * @throws UnsupportedOperationException
     *             <p>
     *             The ICU library was required but was not located. Make sure
     *             that the ICU JAR is on the classpath. See
     *             {@link Options#COLLATOR}.
     *             </p>
     *             <p>
     *             Note: If you are trying to use ICU4JNI then that has to be
     *             locatable as a native library. How you do this is different
     *             for Windows and Un*x.
     *             </p>
     */
    public DefaultKeyBuilderFactory(final Properties properties) {

        // default capacity : @todo config by property.
        this.initialCapacity = 0;
        
        final boolean icu_avail = isICUAvailable();

        if(log.isInfoEnabled()) {

            log.info("ICU library is" + (icu_avail ? "" : " not") + " available.");
            
        }
        
        {
        
            /*
             * Figure out which collator to use.
             */

            collator = CollatorEnum.valueOf(getProperty(properties,
                    Options.COLLATOR, CollatorEnum.ICU.toString()));

            // true iff the collator was _explicitly_ specified.
            final boolean explicitCollatorChoice = getProperty(properties,
                    Options.COLLATOR) != null;

            if (!explicitCollatorChoice) {

                /*
                 * Choice was made by default rather than explicitly specified
                 * by a property.
                 */

                if (log.isInfoEnabled()) {

                    log.info("Defaulting: " + Options.COLLATOR + "="
                            + collator);

                }

            }
            
        }
        
        {

            /*
             * Figure out what Locale to use.
             */
            
            final String language = getProperty(properties, Options.USER_LANGUAGE);
            
            final String country = getProperty(properties, Options.USER_COUNTRY); 
            
            final String variant = getProperty(properties, Options.USER_VARIANT); 
            
            if (language == null) {

                locale = Locale.getDefault();
                
                if( locale == null)
                    throw new AssertionError();
                
            } else {
                
                if (country == null) {

                    locale = new Locale(language);

                } else if (variant == null) {

                    locale = new Locale(language, country);

                } else {

                    locale = new Locale(language, country, variant);

                }

            }

            if (log.isInfoEnabled()) {

                log.info("Using default locale: " + locale.getDisplayName());

            }

        }

        {

            /*
             * Figure out the collator strength.
             */

            Object tmpStrength = null;

            final String val = getProperty(properties, Options.STRENGTH);

            if (val != null) {

                try {

                    tmpStrength = StrengthEnum.valueOf(val);

                } catch (RuntimeException ex) {

                    tmpStrength = Integer.parseInt(val);

                }

            }
            
            if (log.isInfoEnabled())
                log.info(Options.STRENGTH + "=" + tmpStrength);

            /*
             * Note: MAY be null (when null, does not override the collator's
             * default).
             */
            this.strength = tmpStrength;
            
        }

        {
         
            /*
             * Figure out the decomposition mode.
             */
            
            DecompositionEnum mode = null;

            if (getProperty(properties, Options.DECOMPOSITION) != null) {

                mode = DecompositionEnum.valueOf(getProperty(properties,
                        Options.DECOMPOSITION));

                if (log.isInfoEnabled())
                    log.info(Options.DECOMPOSITION + "=" + mode);

            }

            /*
             * Note: MAY be null (when null, does not override the collator's
             * default).
             */
            this.decompositionMode = mode;

        }

        if(log.isInfoEnabled()) {
            
            log.info(toString());
            
        }
        
    }

    public IKeyBuilder getKeyBuilder() {

        if(log.isDebugEnabled()) {
            
            log.debug(toString());
            
        }
        
        return KeyBuilder.newInstance(initialCapacity, collator, locale,
                strength, decompositionMode);

    }
    
    /**
     * Figures out whether or not the ICU library is available.
     * 
     * @return <code>true</code> iff the ICU library is available.
     */
    public static boolean isICUAvailable() {
        
        boolean icu_avail;
        
        try {
        
            Class.forName("com.ibm.icu.text.RuleBasedCollator");
            
            icu_avail = true;
            
        } catch(Throwable t) {
            
            log.warn("ICU library is not available");
            
            icu_avail = false;
            
        }

        return icu_avail;
        
    }
    
}
