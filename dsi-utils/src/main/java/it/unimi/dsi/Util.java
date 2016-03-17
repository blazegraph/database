package it.unimi.dsi;

/*		 
 * DSI utilities
 *
 * Copyright (C) 2002-2009 Sebastiano Vigna 
 *
 *  This library is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 2.1 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 */

import java.util.Enumeration;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/** All-purpose static-method container class.
 *
 * @author Sebastiano Vigna
 * @since 0.1
 */

public final class Util {
	private Util() {}

	/** A reasonable format for real numbers. */
	private static final java.text.NumberFormat FORMAT_DOUBLE = new java.text.DecimalFormat( "#,##0.00" );
	
	/** Formats a number.
	 *
	 * <P>This method formats a double separating thousands and printing just two fractional digits.
	 * @param d a number.
	 * @return a string containing a pretty print of the number.
	 */
	public static String format( final double d ) {
		final StringBuffer s = new StringBuffer();
		return FORMAT_DOUBLE.format( d, s, new java.text.FieldPosition( 0 ) ).toString();
	}
	
	/** A reasonable format for integers. */
	private static final java.text.NumberFormat FORMAT_LONG = new java.text.DecimalFormat( "#,###" );
	
	/** Formats a number.
	 *
	 * <P>This method formats a long separating thousands.
	 * @param l a number.
	 * @return a string containing a pretty print of the number.
	 */
	public static String format( final long l ) {
		final StringBuffer s = new StringBuffer();
		return FORMAT_LONG.format( l, s, new java.text.FieldPosition( 0 ) ).toString();
	}

	/** Formats a size.
	 *
	 * <P>This method formats a long using suitable unit multipliers (e.g., <samp>K</samp>, <samp>M</samp>, <samp>G</samp>, and <samp>T</samp>)
	 * and printing just two fractional digits.
	 * @param l a number, representing a size (e.g., memory).
	 * @return a string containing a pretty print of the number using unit multipliers.
	 */
	public static String formatSize( final long l ) {
		if ( l >= 1000000000000L ) return format( l / 1000000000000.0 ) + "T";
		if ( l >= 1000000000L ) return format( l / 1000000000.0 ) + "G";
		if ( l >= 1000000L ) return format( l / 1000000.0 ) + "M";
		if ( l >= 1000L ) return format( l / 1000.0 ) + "K";
		return Long.toString( l );
	}

	/** Formats a binary size.
	 *
	 * <P>This method formats a long using suitable unit binary multipliers (e.g., <samp>Ki</samp>, <samp>Mi</samp>, <samp>Gi</samp>, and <samp>Ti</samp>)
	 * and printing <em>no</em> fractional digits. The argument must be a power of 2.
	 * @param l a number, representing a binary size (e.g., memory); must be a power of 2.
	 * @return a string containing a pretty print of the number using binary unit multipliers.
	 */
	public static String formatBinarySize( final long l ) {
		if ( ( l & -l ) != l ) throw new IllegalArgumentException( "Not a power of 2: " + l );
		if ( l >= ( 1L << 40 ) ) return format( l >> 40 ) + "Ti";
		if ( l >= ( 1L << 30 ) ) return format( l >> 30 ) + "Gi";
		if ( l >= ( 1L << 20 ) ) return format( l >> 20 ) + "Mi";
		if ( l >= ( 1L << 10 ) ) return format( l >> 10 ) + "Ki";
		return Long.toString( l );
	}

	
	/** Checks whether Log4J is properly configuring by searching for appenders in all loggers.
	 * 
	 * @return whether Log4J is configured (or, at least, an educated guess).
	 */
	
	public static boolean log4JIsConfigured() {
		if ( Logger.getRootLogger().getAllAppenders().hasMoreElements() ) return true;
		Enumeration<?> loggers = LogManager.getCurrentLoggers();
		while ( loggers.hasMoreElements() ) {
			Logger logger = (Logger)loggers.nextElement();
			if ( logger.getAllAppenders().hasMoreElements() ) return true;
		}
		return false;
	}
	
	/** Ensures that Log4J is configured, by invoking, if necessary,
	 * {@link org.apache.log4j.BasicConfigurator#configure()}, and
	 * setting the root logger level to {@link Level#INFO}.
	 * 
	 * @param klass the calling class (to be shown to the user). 
	 */
	
	public static void ensureLog4JIsConfigured( final Class<?> klass ) {
		ensureLog4JIsConfigured( klass, Level.INFO );
	}
	
	/** Ensures that Log4J is configured, by invoking, if necessary,
	 * {@link org.apache.log4j.BasicConfigurator#configure()}, and
	 * setting the root logger level to <code>level</code>.
	 * 
	 * @param klass the calling class (to be shown to the user). 
	 * @param level the required logging level.
	 */
	
	public static void ensureLog4JIsConfigured( final Class<?> klass, final Level level ) {
		if ( ! log4JIsConfigured() ) {
			System.err.println( "WARNING: " + ( klass != null ? klass.getSimpleName()  + " is" : "We are" ) + " autoconfiguring Log4J (level: " + level + "). You should configure Log4J properly instead." );
			BasicConfigurator.configure();
			LogManager.getRootLogger().setLevel( level );
		}
	}
	
	/** Ensures that Log4J is configured, by invoking, if necessary,
	 * {@link org.apache.log4j.BasicConfigurator#configure()}, and
	 * setting the root logger level to {@link Level#INFO}.
	 */
	
	public static void ensureLog4JIsConfigured() {
		ensureLog4JIsConfigured( null, Level.INFO );
	}
	
	/** Ensures that Log4J is configured, by invoking, if necessary,
	 * {@link org.apache.log4j.BasicConfigurator#configure()}, and
	 * setting the root logger level to a specified logging level.
	 * 
	 * @param level the required logging level.
	 */
	
	public static void ensureLog4JIsConfigured( final Level level ) {
		ensureLog4JIsConfigured( null, level );
	}
	
	/** Calls Log4J's {@link Logger#getLogger(java.lang.Class)} method and then {@link #ensureLog4JIsConfigured()}.
	 * 
	 * @param klass a class that will be passed to {@link Logger#getLogger(java.lang.Class)}.
	 * @return the logger returned by {@link Logger#getLogger(java.lang.Class)}.
	 */
	
	public static Logger getLogger( final Class<?> klass ) {
		Logger logger = Logger.getLogger( klass );
		ensureLog4JIsConfigured( klass );
		return logger;
	}
	
	/** Calls Log4J's {@link Logger#getLogger(java.lang.Class)} method and then {@link #ensureLog4JIsConfigured()} with argument {@link Level#DEBUG}.
	 * 
	 * @param klass a class that will be passed to {@link Logger#getLogger(java.lang.Class)}.
	 * @return the logger returned by {@link Logger#getLogger(java.lang.Class)}.
	 */
	
	public static Logger getDebugLogger( final Class<?> klass ) {
		Logger logger = Logger.getLogger( klass );
		ensureLog4JIsConfigured( klass, Level.DEBUG );
		return logger;
	}
	
	private final static Runtime RUNTIME = Runtime.getRuntime();
	
	/** Returns true if less then 5% of the available memory is free.
	 * 
	 * @return true if less then 5% of the available memory is free.
	 */
	public static boolean memoryIsLow() {
		return availableMemory() * 100 < RUNTIME.totalMemory() * 5; 
	}

	/** Returns the amount of available memory (free memory plus never allocated memory).
	 * 
	 * @return the amount of available memory, in bytes.
	 */
	public static long availableMemory() {
		return RUNTIME.freeMemory() + ( RUNTIME.maxMemory() - RUNTIME.totalMemory() ); 
	}

	/** Returns the percentage of available memory (free memory plus never allocated memory).
	 * 
	 * @return the percentage of available memory.
	 */
	public static int percAvailableMemory() {
		return (int)( ( Util.availableMemory() * 100 ) / Runtime.getRuntime().maxMemory() ); 
	}

	/** Tries to compact memory as much as possible by forcing garbage collection.
	 */
	public static void compactMemory() {
		try {
			@SuppressWarnings("unused")
			final byte[][] unused = new byte[ 128 ][]; 
			for( int i = unused.length; i-- != 0; ) unused[ i ] = new byte[ 2000000000 ];
		}
		catch ( OutOfMemoryError itsWhatWeWanted ) {}
		System.gc();
	}
}
