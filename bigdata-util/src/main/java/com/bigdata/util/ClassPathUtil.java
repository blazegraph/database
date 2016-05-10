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
package com.bigdata.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * Utility class for returning an instance of an interface.
 * 
 * @author bryan
 */
public class ClassPathUtil {

	private static final Logger log = Logger.getLogger(ClassPathUtil.class);

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final static private boolean DEBUG = log.isDebugEnabled();
    
    /**
     * BLZG-1703: we cash resolved classes in a map. We use a synchronized
     * map rather than a ConcurrentHashMap since the latter does not support
     * null values (which we use to indicate that resolving failed, e.g. for
     * the optional GPU add-on optimizers).
     */
    final static private Map<ClassPathUtilRequestConfig, Class<?>> cache = 
        Collections.synchronizedMap(new HashMap<ClassPathUtilRequestConfig, Class<?>>());
    
	public static <T> T classForName(final String preferredClassName, final Class<T> defaultClass,
			final Class<T> sharedInterface) {
		
		return classForName(preferredClassName, defaultClass, sharedInterface,
				ClassPathUtil.class.getClassLoader());

	}

	/**
     * Return an instance of the shared interface. If possible, an instance of
     * the preferred class will be used. If that class is not found or is does
     * not extend the specified interface, then an instance of the default class
     * will be used.
     * 
     * @param preferredClassName
     *            The name of the preferred class to use. The class may need to
     *            have a zero argument public constructor in order to be
     *            instantiated by this this method.
     * @param defaultClass
     *            The default class to use (optional). When non-
     *            <code>null</code>, the default class must implement the shared
     *            interface.
     * @param sharedClassOrInterface
     *            A class or interface that the preferred class must implement
     *            if it is to be instantiated.
     * @param classLoader
     *            The class loader to use.
     * 
     * @return An instance of the preferred class if it can be found, implements
     *         the shared interface, and the security checks permit its
     *         instantiation -or- an instance of the default class (if given)
     *         and otherwise <code>null</code>.
     * 
     * @throws IllegalArgumentException
     *             if the preferred class name is <code>null</code>.
     * @throws IllegalArgumentException
     *             if defaultClass is given and does not implement the shared
     *             interface.
     * @throws RuntimeException
     *             if an attempt to instantiate the default class results in a
     *             {@link SecurityException} or {@link IllegalAccessException}.
     */
	@SuppressWarnings("unchecked")
	public static <T> T classForName(final String preferredClassName, final Class<? extends T> defaultClass,
			final Class<T> sharedClassOrInterface, final ClassLoader classLoader) {

	    // throws an IllegalArgumentException if preferredClassName, sharedClassOrInterface,
	    // or classLoader are null
	    final ClassPathUtilRequestConfig requestConfig = 
	        new ClassPathUtilRequestConfig(
	            preferredClassName, defaultClass, sharedClassOrInterface, classLoader);

	       try {

    	    // first try lookup in cache and take early exit if present
    	    if (cache.containsKey(requestConfig)) {
    
    	        final Class<?> cls = cache.get(requestConfig);
    	        
    	        return cls == null ? null : (T) cls.newInstance();
    	    }
    	    
    	    
    		if (defaultClass != null && !sharedClassOrInterface.isAssignableFrom(defaultClass)) {
    			// The default class must extend the shared interface.
    			throw new IllegalArgumentException();
    		}
			
			// Do not initialize the class when it is loaded.
			final boolean initialize = false;

			// Use the caller's class loader to find the preferred class.
			final Class<?> cls = Class.forName(preferredClassName, initialize, classLoader);

			if (sharedClassOrInterface.isAssignableFrom(cls)) {

				// Found preferred class.  Is instance of shared interface.
				if (log.isInfoEnabled()) {
					log.info("Found " + cls.getCanonicalName());
				}

				// remember for next lookup in cache
				cache.put(requestConfig, cls);
				
				// Return instance of preferred class.
				return (T) cls.newInstance();

			}

			// Class is not instance of shared interface. Can not use.  Will return default class instance.
			log.warn(cls.getCanonicalName() + " does not extend " + sharedClassOrInterface.getCanonicalName());

			// fall through
		
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {

			// Could not find preferred class.  Will use default instance.  Do NOT log @ WARN.
			
			if (DEBUG) {
				log.debug("Not found: " + preferredClassName);
			}
			
			// fall through
			
		}

		/*
		 * Return an instance of the default class (if given).
		 */

		if (defaultClass == null) {

            // remember for next lookup in cache
            cache.put(requestConfig, null);
		    
			// If there is no default class, return null.
			return null;

		}

		try {

			if (DEBUG) {
				log.debug("Using defaultClass: " + defaultClass.getCanonicalName());
			}
			
            // remember for next lookup in cache
            cache.put(requestConfig, defaultClass);

			// Return an instance of the default class.
			return (T) defaultClass.newInstance();

		} catch (InstantiationException | IllegalAccessException e) {

			throw new RuntimeException(e);

		}

	}
	
	/**
     * Configuration representing a request for a given class based
     * on preferred name, default class, shared class or instance, and the
     * class loader to be used.
     * 
     * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
	 */
	private static class ClassPathUtilRequestConfig {
	    
	    final protected String preferredClassName;
	    final protected Class<?> defaultClass;
        final protected Class<?> sharedClassOrInterface;
        final protected ClassLoader classLoader;
        
        /**
         * Initialize the config. preferredClassName, sharedClassOrInterface, and
         * the classLoader must be non null, otherwise and {@link IllegalArgumentException}
         * is thrown.
         * 
         * @param preferredClassName
         * @param defaultClass
         * @param sharedClassOrInterface
         * @param classLoader
         */
        public ClassPathUtilRequestConfig(
            final String preferredClassName, final Class<?> defaultClass,
            final Class<?> sharedClassOrInterface, final ClassLoader classLoader) 
        throws IllegalArgumentException {
            
            if (preferredClassName == null)
                throw new IllegalArgumentException();

            if (sharedClassOrInterface == null)
                throw new IllegalArgumentException();
            
            if (classLoader == null)
                throw new IllegalArgumentException();
            
            this.preferredClassName = preferredClassName;
            this.defaultClass = defaultClass;
            this.sharedClassOrInterface = sharedClassOrInterface;
            this.classLoader = classLoader;
        }
        
        @Override
        public int hashCode() {
            
            int hashCode = 1;
            hashCode = 37 * hashCode + preferredClassName.hashCode();
            hashCode = 37 * hashCode + sharedClassOrInterface.hashCode();
            hashCode = 37 * hashCode + classLoader.hashCode();
            
            if (defaultClass!=null) {
                hashCode = 37 * hashCode + defaultClass.hashCode();                
            }
            
            return hashCode;
        }
        
        @Override
        public boolean equals(Object other) {
            
            if (other==null || !(other instanceof ClassPathUtilRequestConfig)) {
                return false;
            }
            
            final ClassPathUtilRequestConfig otherAsConfig = (ClassPathUtilRequestConfig)other;
            
            boolean equals = true;
            
            // preferredClassName non null by construction
            equals &= preferredClassName.equals(otherAsConfig.preferredClassName);
            
            // sharedClassOrInterface non null by construction
            equals &= sharedClassOrInterface.equals(otherAsConfig.sharedClassOrInterface);

            // classLoader non null by construction
            equals &= classLoader.equals(otherAsConfig.classLoader);
            
            // default class may be null
            equals &= defaultClass==null ?
                    otherAsConfig.defaultClass==null :
                        defaultClass.equals(otherAsConfig.defaultClass);


            return equals;
            
        }
	}

}
