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
package com.bigdata.util;

import org.apache.log4j.Logger;

/**
 * Utility class for returning an instance of an interface.
 * 
 * @author bryan
 */
public class ClassPathUtil {

	private static final Logger log = Logger.getLogger(ClassPathUtil.class);

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
	 * @param sharedInterface
	 *            An interface that the preferred class must implement if it is
	 *            to be instantiated.
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
			final Class<T> sharedInterface, final ClassLoader classLoader) {

		if (preferredClassName == null)
			throw new IllegalArgumentException();

		if (sharedInterface == null)
			throw new IllegalArgumentException();
		
		if (classLoader == null)
			throw new IllegalArgumentException();

		if (defaultClass != null && !sharedInterface.isAssignableFrom(defaultClass)) {
			// The default class must extend the shared interface.
			throw new IllegalArgumentException();
		}

		try {
			
			// Do not initialize the class when it is loaded.
			final boolean initialize = false;

			// Use the caller's class loader to find the preferred class.
			final Class<?> cls = Class.forName(preferredClassName, initialize, classLoader);

			if (sharedInterface.isAssignableFrom(cls)) {

				// Found preferred class.  Is instance of shared interface.
				if (log.isInfoEnabled()) {
					log.info("Found " + cls.getCanonicalName());
				}

				// Return instance of preferred class.
				return (T) cls.newInstance();

			}

			// Class is not instance of shared interface. Can not use.  Will return default class instance.
			log.warn(cls.getCanonicalName() + " does not extend " + sharedInterface.getCanonicalName());

			// fall through
		
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {

			// Could not find preferred class.  Will use default instance.  Do NOT log @ WARN.
			
			if (log.isInfoEnabled()) {
				log.info("Not found: " + preferredClassName);
			}
			
			// fall through
			
		}

		/*
		 * Return an instance of the default class (if given).
		 */

		if (defaultClass == null) {

			// If there is no default class, return null.
			return null;

		}

		try {

			if (log.isInfoEnabled()) {
				log.info("Using defaultClass: " + defaultClass.getCanonicalName());
			}

			// Return an instance of the default class.
			return (T) defaultClass.newInstance();

		} catch (InstantiationException | IllegalAccessException e) {

			throw new RuntimeException(e);

		}

	}

}
