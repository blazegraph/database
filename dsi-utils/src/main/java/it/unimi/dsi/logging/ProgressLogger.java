package it.unimi.dsi.logging;

/*		 
 * DSI utilities
 *
 * Copyright (C) 2005-2009 Sebastiano Vigna 
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

import it.unimi.dsi.Util;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;


/** Tunable progress logger.
 *
 * <P>This class provides a simple way to log progress information about long-lasting activities.
 *
 * <P>To use this class, you first create a new instance by passing a 
 * {@linkplain org.apache.log4j.Logger Log4J logger}, a {@link org.apache.log4j.Priority}
 * and a time interval in millisecond (you can use constants such as {@link #ONE_MINUTE}).
 * Information will be logged about the current state of affairs no more often than the given
 * time interval. The output of the logger depends on
 * the {@linkplain #itemsName items name} (the name that will be used to denote counted
 * items), which can be changed at any time.
 * 
 * <P>To log the progress of an activity, you call {@link #start(CharSequence)} at the beginning, which will
 * display the given string. Then, each time you want to mark progress, you call {@link #update()} or {@link #lightUpdate()}.
 * The latter methods increase the item counter, and will log progress information if enough time
 * has passed since the last log (and if the counter is a multiple of 2<sup>{@link #log2Modulus}</sup>, in the case of {@link #lightUpdate()}).
 * 
 * When the activity is over, you call {@link #stop()}. At that point, the method {@link #toString()} returns
 * information about the internal state of the logger (elapsed time, number of items per second) that
 * can be printed or otherwise processed. If {@link #update()} has never been called, you will just
 * get the elapsed time. By calling {@link #done()} instead of stop, this information will be logged for you.
 *
 * <P>Additionally, by setting the {@linkplain #expectedUpdates expected amount of updates} before
 * calling {@link #start()} you can get some estimations on the completion time.
 *  
 * <P>After you finished a run of the progress logger, 
 * you can change its attributes and call {@link #start()} again
 * to measure another activity.
 *
 * <P>A typical call sequence to a progress logger is as follows:
 * <PRE>
 * ProgressLogger pl = new ProgressLogger( logger, ProgressLogger.ONE_MINUTE );
 * pl.start("Smashing pumpkins...");
 * ... activity on pumpkins that calls update() on each pumpkin ...
 * pl.done();
 * </PRE>
 *
 * <P>A more flexible behaviour can be obtained at the end of the
 * process by calling {@link #stop()}:
 * <PRE>
 * ProgressLogger pl = new ProgressLogger( logger, ProgressLogger.ONE_MINUTE, "pumpkins" );
 * pl.start("Smashing pumpkins...");
 * ... activity on pumpkins that calls update() on each pumpkin ...
 * pl.stop( "Really done!" );
 * pl.logger.log( pl.priority, pm );
 * </PRE>
 * 
 * <P>Should you need to display additional information, you can set the field {@link #info} to any
 * object: it will be printed just after the timing (and possibly memory) information.
 * 
 * <P>Note that the {@linkplain org.apache.log4j.Logger Log4J logger} and 
 * priority are available via the public fields {@link #logger} and 
 * {@link #priority}: this makes it possible to pass around a progress logger and log additional information on
 * the same logging stream. The priority is initialised to {@link Level#INFO}, but it can be set at any time.
 * 
 * @author Sebastiano Vigna
 * @since 0.9.3
 */

public final class ProgressLogger {
	public final static long ONE_SECOND = 1000;
	public final static long TEN_SECONDS = 10 * ONE_SECOND;
	public final static long ONE_MINUTE = ONE_SECOND * 60;
	public final static long TEN_MINUTES = ONE_MINUTE * 10;
	public final static long ONE_HOUR = ONE_MINUTE * 60;
	public final static long DEFAULT_LOG_INTERVAL = TEN_SECONDS;

	private static final Runtime RUNTIME = Runtime.getRuntime();

	/** The default {@link #log2Modulus} for {@link #lightUpdate()}. */
	public final int DEFAULT_LOG2_MODULUS = 10;
	
	/** The logger used by this progress logger. */
	final public Logger logger;
	/** The priority used by this progress logger. It can be changed at any time. */
	public Level priority = Level.INFO;
	/** The time interval for a new log in milliseconds. */
	public long logInterval;
	/** Unused.
	 * @deprecated Replaced by {@link #log2Modulus}, which avoids the very slow modulus operator at each {@link #lightUpdate()} call.
	 */
	@Deprecated
	public int modulus = 1000;
	/** If nonzero, calls to {@link #lightUpdate()} will cause a call to
	 * {@link System#currentTimeMillis()} only if the current value of {@link #count}
	 * is a multiple of 2 raised to this power. */
	public int log2Modulus = DEFAULT_LOG2_MODULUS;
	/** If non-<code>null</code>, this object will be printed after the timinig information. */
	public Object info;
	/** The number of calls to {@link #update()} since the last {@link #start()}. */
	public long count;
	/** The number of expected calls to {@link #update()} (used to compute the percentages, ignored if negative). */
	public long expectedUpdates;
	/** The name of several counted items. */
	public String itemsName;
	/** Whether to display the free memory at each progress log (default: false). */
	public boolean displayFreeMemory;
	
	/** The time at the last call to {@link #start()}. */
	private long start;
	/** The time at the last call to {@link #stop()}. */
	private long stop;
	/** The time of the last log. */
	private long lastLog;

	/** Creates a new progress logger using <samp>items</samp> as items name and logging every 
	 * {@link #DEFAULT_LOG_INTERVAL} milliseconds with
	 * to the {@linkplain Logger#getRootLogger() root logger}.
	 */
	public ProgressLogger() {
		this( Logger.getRootLogger() );
	}

	/** Creates a new progress logger using <samp>items</samp> as items name and logging every {@link #DEFAULT_LOG_INTERVAL} milliseconds. 
	 *
	 * @param logger the logger to which messages will be sent.
	 */
	public ProgressLogger( final Logger logger ) {
		this( logger, DEFAULT_LOG_INTERVAL );
	}
	 
	/** Creates a new progress logger logging every {@link #DEFAULT_LOG_INTERVAL} milliseconds. 
	 *
	 * @param logger the logger to which messages will be sent.
	 * @param itemsName a plural name denoting the counted items.
	 */
	public ProgressLogger( final Logger logger, final String itemsName ) {
		this( logger, DEFAULT_LOG_INTERVAL, itemsName );
	}
	 
	/** Creates a new progress logger using <samp>items</samp> as items name. 
	 *
	 * @param logger the logger to which messages will be sent.
	 * @param logInterval the logging interval in milliseconds.
	 */
	public ProgressLogger( final Logger logger, final long logInterval ) {
		this( logger, logInterval, "items" );
	}
	 
	/** Creates a new progress logger. 
	 *
	 * @param logger the logger to which messages will be sent.
	 * @param logInterval the logging interval in milliseconds.
	 * @param itemsName a plural name denoting the counted items.
	 */
	public ProgressLogger( final Logger logger, final long logInterval, final String itemsName ) {
		this.logger = logger;
		this.logInterval = logInterval;
		this.itemsName = itemsName;
		this.expectedUpdates = -1;
	}
	 
	/** Updates the progress logger.
	 *
	 * <P>This call updates the progress logger internal count. If enough time has passed since the
	 * last log, information will be logged.
	 * 
	 * <p>This method is kept intentionally short (it delegates most of the work to an internal
	 * private method) so to suggest inlining. However, it performs a call to {@link System#currentTimeMillis()}
	 * that takes microseconds (not nanoseconds). If you plan on calling this method more than a 
	 * few thousands times per second, you should use {@link #lightUpdate()}. 
	 */
	public void update() {
		count++;
		if ( System.currentTimeMillis() - lastLog >= logInterval ) updateInternal(); 
	}

	private String freeMemory() {
		return ( displayFreeMemory ? "; used/avail/free/total/max mem: " 
				+ Util.formatSize( RUNTIME.totalMemory() - RUNTIME.freeMemory() ) + "/" 
				+ Util.formatSize( RUNTIME.freeMemory() + ( RUNTIME.maxMemory() - RUNTIME.totalMemory() ) ) + "/" 
				+ Util.formatSize( RUNTIME.freeMemory() ) + "/" 
				+ Util.formatSize( RUNTIME.totalMemory() ) + "/" 
				+ Util.formatSize( RUNTIME.maxMemory() ) : "" );
	}
	
	private void updateInternal() {
		final long currentTime = System.currentTimeMillis();
		final long millisToEnd = Math.round( ( expectedUpdates - count ) * ( ( currentTime - start ) / ( count + 1.0 ) ) );
		// Formatting is expensive, so we check for actual logging.
		if ( logger.isEnabledFor( priority ) ) 
			logger.log( priority, Util.format( count ) + " " + itemsName + ", " + 
					millis2hms( millis() ) + ", " + Util.format( ( count * 1000.0 ) / ( currentTime - start ) ) + " " + itemsName + 
					"/s"  + ( expectedUpdates > 0 ? "; " + Util.format( ( 100 * count ) / expectedUpdates ) + "% done, " + 
					millis2hms( millisToEnd ) + " to end" : "" ) + freeMemory() + ( info != null ? "; " + info : "" ) );

		lastLog = currentTime;
		return;
	}

	/** Updates the progress logger in a lightweight fashion.
	 *
	 * <P>This call updates the progress logger internal counter as {@link #update()}. However, 
	 * it will actually call {@link System#currentTimeMillis()} only if the new {@link #count}
	 * is a multiple of 2<sup>{@link #log2Modulus}</sup>. This mechanism makes it possible to reduce the number of
	 * calls to {@link System#currentTimeMillis()} arbitrarily.
	 * 
	 * <p>This method is useful when the operations being counted take less than a few microseconds.
	 * 
	 * @see #update()
	 */

	public final void lightUpdate() {
		if ( ( ++count & ( 1L << log2Modulus ) - 1 )  == 0 && System.currentTimeMillis() - lastLog >= logInterval ) updateInternal(); 
	}


	/** Starts the progress logger, displaying a message and resetting the count.
	 * @param message the message to display.
	 */

	public void start( final CharSequence message ) {
		if ( message != null ) logger.log( priority, message );
		start = lastLog = System.currentTimeMillis();
		count = 0;
		stop = -1;
	}
	
	/** Starts the progress logger, resetting the count.*/

	public void start() { start( null ); }

	/** Stops the progress logger, displaying a message.
	 * 
	 * <p>This method will also mark {@link #expectedUpdates} as invalid,
	 * to avoid erroneous reuses of previous values.
	 *
	 * @param message the message to display.
	 */

	public void stop( final CharSequence message ) {
		if ( stop != -1 ) return;
		if ( message != null ) logger.log( priority, message );
		stop = System.currentTimeMillis();
		expectedUpdates = -1;
	}

	/** Stops the progress logger. */

	public void stop() { stop( null ); }

	/** Completes a run of this progress logger, logging <samp>Completed&#46;</samp> and the logger itself. */
	public void done() {
		stop( "Completed." );
		logger.log( priority, this );
	}

	/** Returns the number of milliseconds between present time and the last call to {@link #start()}, if
	 * this progress logger is running, or between the last call to {@link #stop()} and the last call to {@link #start()}, if this 
	 * progress logger is stopped.
	 * 
	 * @return the number of milliseconds between present time and the last call to {@link #start()}, if
	 * this progress logger is running, or between the last call to {@link #stop()} and the last call to {@link #start()}, if this 
	 * progress logger is stopped.
	 */

	public long millis() {
		if ( stop != -1 ) return stop - start;
		else return System.currentTimeMillis() - start;
	}

	private String millis2hms( final long t ) {
		if ( t < 1000 ) return t + "ms";
		final long s = ( t / 1000 ) % 60;
		final long m = ( ( t / 1000 ) / 60 ) % 60;
		final long h = t / ( 3600 * 1000 );
		if ( h == 0 && m == 0 ) return s + "s";
		if ( h == 0 ) return m + "m " + s + "s";
		return h + "h " + m + "m " + s + "s";
	}


	/** Converts the data stored in this progress logger to a string. 
	 * 
	 * @return the data in this progress logger in a printable form.
	 */
	
	public String toString() {
		final long t = stop - start + 1 ;
	
		if ( t <= 0 ) return "Illegal progress logger state";

		return "Elapsed: " + millis2hms( t ) + ( count != 0 ? " [" + Util.format( count ) + " " + itemsName + ", " + Util.format( count / ( t / 1000.0 ) ) + " " + itemsName + "/s]" : "" );
	}
}
