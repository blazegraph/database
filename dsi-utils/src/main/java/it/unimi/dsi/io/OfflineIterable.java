package it.unimi.dsi.io;


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
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.objects.AbstractObjectIterator;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;

/** An iterable that offers elements that were previously stored offline using specialized
 *  serialization methods. At construction, you provide a {@linkplain #OfflineIterable(it.unimi.dsi.io.OfflineIterable.Serializer, Object) serializer}
 *  that establishes how elements are written offline; after that, you can
 *  {@linkplain #add(Object) add elements} one at a time or in a {@linkplain #addAll(Iterable) bulk way}. 
 *  At any moment, you can {@linkplain #iterator() get} an {@link OfflineIterable.OfflineIterator OfflineIterator} 
 *  on this object that returns all the elements added so far. Note that the returned iterator caches the current number of elements,
 *  so each iterator will return just the elements added at the time of its creation.
 *  
 *  <p><strong>Warning</strong>: The store object provided at {@linkplain OfflineIterable#OfflineIterable(it.unimi.dsi.io.OfflineIterable.Serializer, Object ) 
 *  construction time} is shared by all iterators.
 * 
 *  <h2>Closing</h2>
 *  
 *  <p>Both {@link OfflineIterable} and {@link OfflineIterable.OfflineIterator OfflineIterator} are {@link SafelyCloseable} (the latter will
 *  close its input stream when <code>hasNext()</code> returns false), but for better resource management you should close them after usage.
 *  
 *  <h2>Store reuse</h2>
 *  
 * @author Sebastiano Vigna
 * @since 0.9.2
 */
public class OfflineIterable<T,U extends T> implements Iterable<U>,SafelyCloseable {
    public static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Util.getLogger( OfflineIterable.class );
	
    /** An iterator returned by an {@link OfflineIterable}. */
	public final static class OfflineIterator<A, B extends A> extends AbstractObjectIterator<B> implements SafelyCloseable {
		/** The data input stream that accesses the file of the related {@link OfflineIterable}. */
		private final DataInputStream dis;
		/** The number of elements in the related {@link OfflineIterable}. */
		private final long size;
		/** The serializer used to store and read the elements of this iterable. */
		private final Serializer<? super A, B> serializer;
		/** An object that is (re)used by the iterator(s) iterating on this iterable. */
		private final B store;
		/** The number of elements read by this iterator. */
		private long read;
		/** Whether this iterator has been closed. */
		private boolean closed = false;

		private OfflineIterator( DataInputStream dis, final Serializer<? super A, B> serializer, B store, long size ) {
			this.dis = dis;
			this.serializer = serializer;
			this.store = store;
			this.size = size;
		}

		public boolean hasNext() {
			if ( read >= size ) close();
			return read < size;
		}

		public B next() {
			if ( !hasNext() ) throw new NoSuchElementException();
			try {
				serializer.read( dis, store );
			}
			catch ( IOException e ) {
				throw new RuntimeException( e );
			}
			read++;
			return store;
		}

		public void close() {
			if ( !closed ) {
				try {
					dis.close();
				}
				catch ( IOException e ) {
					throw new RuntimeException( e );
				}
				closed = true;
			}
		}
		
		protected void finalize() throws Throwable {
			try {
				if ( ! closed ) {
					LOGGER.warn( "This " + this.getClass().getName() + " [" + toString() + "] should have been closed." );
					close();
				}
			}
			finally {
				super.finalize();
			}
		}
	}

	/** Determines a strategy to serialize and deserialize elements.
	 */
	public interface Serializer<A, B extends A> {
		/** Writes out an element.
		 * 
		 * @param x the element to be written.
		 * @param dos the stream where the element should be written.
		 * @throws IOException if an exception occurs while writing.
		 */
		public void write( A x, DataOutputStream dos ) throws IOException;
		
		/** Reads an element.
		 * 
		 * @param dis the stream whence the element should be read.
		 * @param x the object where the element will be read.
		 * @throws IOException if an exception occurs while reading.
		 */
		public void read( DataInputStream dis, B x ) throws IOException;
	}

	/** The serializer used to store and read the elements of this iterable. */
	private final Serializer<? super T, U> serializer;
	/** The file where elements are serialized. */
	private final File file;
	/** A data output stream associated with {@link #file}. */
	private final DataOutputStream dos;
	/** An object that is (re)used by the iterator(s) iterating on this iterable. */
	private final U store;
	/** The number of elements written so far. */
	private long size;
	/** Whether this iterable has been closed. */
	private boolean closed = false;


	/** Creates an offline iterable with given serializer.
	 * 
	 * @param serializer the serializer to be used.
	 * @param store an object that is (re)used by the iterator(s) iterating on this iterable.
	 * @throws IOException 
	 */
	public OfflineIterable( final Serializer<? super T, U> serializer, final U store ) throws IOException {
		this.serializer = serializer;
		this.store = store;
		file = File.createTempFile( OfflineIterable.class.getSimpleName(), "elmts" );
		file.deleteOnExit();
		dos = new DataOutputStream( new FastBufferedOutputStream( new FileOutputStream( file ) ) );
	}
	

	/** Adds a new element at the end of this iterable.
	 * 
	 * @param x the element to be added.
	 * @throws IOException 
	 */
	public void add( T x ) throws IOException {
		serializer.write( x, dos );
		size++;
	}
	
	/** Adds all the elements of the given iterable at the end of this iterable. 
	 * 
	 * @param it the iterable producing the elements to be added.
	 * @throws IOException
	 */
	public void addAll( Iterable<T> it ) throws IOException {
		for ( T x: it ) add( x );
	}

	public OfflineIterator<T, U> iterator() {
		try {
			dos.flush();
			final DataInputStream dis = new DataInputStream( new FastBufferedInputStream( new FileInputStream( file ) ) );
			return new OfflineIterator<T, U>( dis, serializer, store, size );
		}
		catch ( IOException e ) {
			throw new RuntimeException( e );
		}
	}


	public void close() {
		if ( !closed ) {
			try {
				dos.close();
				file.delete();
			}
			catch ( IOException e ) {
				throw new RuntimeException( e );
			}
			closed = true;
		}
	}
	
	protected void finalize() throws Throwable {
		try {
			if ( ! closed ) {
				LOGGER.warn( "This " + this.getClass().getName() + " [" + toString() + "] should have been closed." );
				close();
			}
		}
		finally {
			super.finalize();
		}
	}

	/** Returns the number of elements added so far, unless it is too big to fit in an integer (in which case this method will throw an
	 *  exception).
	 * 
	 * @return the number of elements added so far.
	 */
	public int size() {
		final long length = length();
		if ( length > Integer.MAX_VALUE ) throw new IllegalStateException( "The number of elements of this bit list (" + length + ") exceeds Integer.MAX_INT" );
		return (int)length;
	}

	/** Returns the number of elements added so far.
	 * 
	 * @return the number of elements added so far.
	 */
	public long length() {
		return size;
	}
}
