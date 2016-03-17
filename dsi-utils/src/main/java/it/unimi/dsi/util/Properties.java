package it.unimi.dsi.util;

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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.Writer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.ConfigurationMap;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.PropertiesConfiguration;

/** An extension of {@link org.apache.commons.configuration.PropertiesConfiguration}
 * providing setters for primitive types, a simpler {@linkplain #save(CharSequence) way to save preferences}
 * and transparent handling of {@link java.lang.Enum} lowercased keys.
 * 
 * <p>All accessors defined in {@link org.apache.commons.configuration.PropertiesConfiguration} have a
 * polymorphic counterpart taking an {@link java.lang.Enum} instead of a string: {@link java.lang.Enum#name()}
 * <strong>and {@link java.lang.String#toLowerCase()}</strong> are applied before 
 * delegating to the corresponding string-based method. (This apparently wierd choice is due to the need to
 * accommodate the upper-case standard for {@link java.lang.Enum} elements and the lower-case standard
 * for property keys.) 
 * 
 * <p>Additionally, instances of this class can be serialised.
 */
public class Properties extends PropertiesConfiguration implements Serializable {

	private static final long serialVersionUID = 1L;

	public Properties() {}

	public Properties( final String filename ) throws ConfigurationException {
		super( filename );
	}

	public Properties( final File file ) throws ConfigurationException {
		super( file );
	}

	public Properties( final URL url ) throws ConfigurationException {
		super( url );
	}

	/** Saves the configuration to the specified file.
	 * 
	 * @param filename a file name.
	 */
	
	public void save( final CharSequence filename ) throws ConfigurationException, IOException {
		final Writer w = new OutputStreamWriter( new FileOutputStream( filename.toString() ), "UTF-8" );
		super.save( w );
		w.close();
	}

	/** Adds all properties from the given configuration. 
	 * 
	 * <p>Properties from the new configuration will clear properties from the first one.
	 * 
	 * @param configuration a configuration.
	 * */
	@SuppressWarnings("unchecked")
	public void addAll( final Configuration configuration ) {
		new ConfigurationMap( this ).putAll( new ConfigurationMap( configuration ) );
	}

	// Methods to add properties represented by primitive types easily
	
	public void addProperties( final String key, final String[] s ) {
		for( int i = 0; i < s.length; i++ )
			super.addProperty( key, s[ i ] );
	}
	
	public void addProperty( final String key, final boolean b ) {
		super.addProperty( key, Boolean.valueOf( b ) );
	}
	
	public void setProperty( final String key, final boolean b ) {
		super.setProperty( key, Boolean.valueOf( b ) );
	}
	
	public void addProperty( final String key, final byte b ) {
		super.addProperty( key, Byte.valueOf( b ) );
	}
	
	public void setProperty( final String key, final byte b ) {
		super.setProperty( key, Byte.valueOf( b ) );
	}
	
	public void addProperty( final String key, final short s ) {
		super.addProperty( key, Short.valueOf( s ) );
	}
	
	public void setProperty( final String key, final short s ) {
		super.setProperty( key, Short.valueOf( s ) );
	}
	
	public void addProperty( final String key, final char c ) {
		super.addProperty( key, Character.valueOf( c ) );
	}
	
	public void setProperty( final String key, final char b ) {
		super.setProperty( key, Character.valueOf( b ) );
	}
	
	public void addProperty( final String key, final int i ) {
		super.addProperty( key, Integer.valueOf( i ) );
	}
	
	public void setProperty( final String key, final int i ) {
		super.setProperty( key, Integer.valueOf( i ) );
	}
	
	public void addProperty( final String key, final long l ) {
		super.addProperty( key, Long.valueOf( l ) );
	}
	
	public void setProperty( final String key, final long l ) {
		super.setProperty( key, Long.valueOf( l ) );
	}
	
	public void addProperty( final String key, final float f ) {
		super.addProperty( key, Float.valueOf( f ) );
	}
	
	public void setProperty( final String key, final float f ) {
		super.setProperty( key, Float.valueOf( f ) );
	}
	
	public void addProperty( final String key, final double d ) {
		super.addProperty( key, Double.valueOf( d ) );
	}
	
	public void setProperty( final String key, final double d ) {
		super.setProperty( key, Double.valueOf( d ) );
	}
	
	// Same methods, but with Enum keys
	
	public void addProperties( final Enum<?> key, final String[] s ) {
		for( int i = 0; i < s.length; i++ )
			super.addProperty( key.name().toLowerCase(), s[ i ] );
	}
	
	public void addProperty( final Enum<?> key, final boolean b ) {
		super.addProperty( key.name().toLowerCase(), Boolean.valueOf( b ) );
	}
	
	public void setProperty( final Enum<?> key, final boolean b ) {
		super.setProperty( key.name().toLowerCase(), Boolean.valueOf( b ) );
	}
	
	public void addProperty( final Enum<?> key, final byte b ) {
		super.addProperty( key.name().toLowerCase(), Byte.valueOf( b ) );
	}
	
	public void setProperty( final Enum<?> key, final byte b ) {
		super.setProperty( key.name().toLowerCase(), Byte.valueOf( b ) );
	}
	
	public void addProperty( final Enum<?> key, final short s ) {
		super.addProperty( key.name().toLowerCase(), Short.valueOf( s ) );
	}
	
	public void setProperty( final Enum<?> key, final short s ) {
		super.setProperty( key.name().toLowerCase(), Short.valueOf( s ) );
	}
	
	public void addProperty( final Enum<?> key, final char c ) {
		super.addProperty( key.name().toLowerCase(), Character.valueOf( c ) );
	}
	
	public void setProperty( final Enum<?> key, final char b ) {
		super.setProperty( key.name().toLowerCase(), Character.valueOf( b ) );
	}
	
	public void addProperty( final Enum<?> key, final int i ) {
		super.addProperty( key.name().toLowerCase(), Integer.valueOf( i ) );
	}
	
	public void setProperty( final Enum<?> key, final int i ) {
		super.setProperty( key.name().toLowerCase(), Integer.valueOf( i ) );
	}
	
	public void addProperty( final Enum<?> key, final long l ) {
		super.addProperty( key.name().toLowerCase(), Long.valueOf( l ) );
	}
	
	public void setProperty( final Enum<?> key, final long l ) {
		super.setProperty( key.name().toLowerCase(), Long.valueOf( l ) );
	}
	
	public void addProperty( final Enum<?> key, final float f ) {
		super.addProperty( key.name().toLowerCase(), Float.valueOf( f ) );
	}
	
	public void setProperty( final Enum<?> key, final float f ) {
		super.setProperty( key.name().toLowerCase(), Float.valueOf( f ) );
	}
	
	public void addProperty( final Enum<?> key, final double d ) {
		super.addProperty( key.name().toLowerCase(), Double.valueOf( d ) );
	}
	
	public void setProperty( final Enum<?> key, final double d ) {
		super.setProperty( key.name().toLowerCase(), Double.valueOf( d ) );
	}

	// Polimorphic Enum version of superclass string-based methods
	
	public boolean containsKey( final Enum<?> key ) {
		return containsKey( key.name().toLowerCase() );
	}
	
	public Object getProperty( final Enum<?> key ) {
		return getProperty( key.name().toLowerCase() );
	}

	public void addProperty( final Enum<?> key, Object arg ) {
		addProperty( key.name().toLowerCase(), arg );
	}
	
	public BigDecimal getBigDecimal( final Enum<?> key, BigDecimal arg ) {
		return getBigDecimal( key.name().toLowerCase(), arg );
	}
	
	public BigDecimal getBigDecimal( final Enum<?> key ) {
		return getBigDecimal( key.name().toLowerCase() );
	}
	
	public BigInteger getBigInteger( final Enum<?> key, BigInteger arg ) {
		return getBigInteger( key.name().toLowerCase(), arg );
	}
	
	public BigInteger getBigInteger( final Enum<?> key ) {
		return getBigInteger( key.name().toLowerCase() );
	}
	
	public boolean getBoolean( final Enum<?> key, boolean arg ) {
		return getBoolean( key.name().toLowerCase(), arg );
	}
	
	public Boolean getBoolean( final Enum<?> key, Boolean arg ) {
		return getBoolean( key.name().toLowerCase(), arg );
	}
	
	public boolean getBoolean( final Enum<?> key ) {
		return getBoolean( key.name().toLowerCase() );
	}
	
	public byte getByte( final Enum<?> key, byte arg ) {
		return getByte( key.name().toLowerCase(), arg );
	}
	
	public Byte getByte( final Enum<?> key, Byte arg ) {
		return getByte( key.name().toLowerCase(), arg );
	}
	
	public byte getByte( final Enum<?> key ) {
		return getByte( key.name().toLowerCase() );
	}
	
	public double getDouble( final Enum<?> key, double arg ) {
		return getDouble( key.name().toLowerCase(), arg );
	}
	
	public Double getDouble( final Enum<?> key, Double arg ) {
		return getDouble( key.name().toLowerCase(), arg );
	}
	
	public double getDouble( final Enum<?> key ) {
		return getDouble( key.name().toLowerCase() );
	}
	
	public float getFloat( final Enum<?> key, float arg ) {
		return getFloat( key.name().toLowerCase(), arg );
	}
	
	public Float getFloat( final Enum<?> key, Float arg ) {
		return getFloat( key.name().toLowerCase(), arg );
	}
	
	public float getFloat( final Enum<?> key ) {
		return getFloat( key.name().toLowerCase() );
	}
	
	public int getInt( final Enum<?> key, int arg ) {
		return getInt( key.name().toLowerCase(), arg );
	}
	
	public int getInt( final Enum<?> key ) {
		return getInt( key.name().toLowerCase() );
	}
	
	public Integer getInteger( final Enum<?> key, Integer arg ) {
		return getInteger( key.name().toLowerCase(), arg );
	}
	
	public Iterator<?> getKeys( final Enum<?> key ) {
		return getKeys( key.name().toLowerCase() );
	}
	
	public List<?> getList( final Enum<?> key, List<?> arg ) {
		return getList( key.name().toLowerCase(), arg );
	}
	
	public List<?> getList( final Enum<?> key ) {
		return getList( key.name().toLowerCase() );
	}
	
	public long getLong( final Enum<?> key, long arg ) {
		return getLong( key.name().toLowerCase(), arg );
	}
	
	public Long getLong( final Enum<?> key, Long arg ) {
		return getLong( key.name().toLowerCase(), arg );
	}
	
	public long getLong( final Enum<?> key ) {
		return getLong( key.name().toLowerCase() );
	}
	
	public java.util.Properties getProperties( final Enum<?> key, java.util.Properties arg ) {
		return getProperties( key.name().toLowerCase(), arg );
	}

	public java.util.Properties getProperties( final Enum<?> key ) {
		return getProperties( key.name().toLowerCase() );
	}
	
	public short getShort( final Enum<?> key, short arg ) {
		return getShort( key.name().toLowerCase(), arg );
	}
	
	public Short getShort( final Enum<?> key, Short arg ) {
		return getShort( key.name().toLowerCase(), arg );
	}
	
	public short getShort( final Enum<?> key ) {
		return getShort( key.name().toLowerCase() );
	}
	
	public String getString( final Enum<?> key, String arg ) {
		return getString( key.name().toLowerCase(), arg );
	}
	
	public String getString( final Enum<?> key ) {
		return getString( key.name().toLowerCase() );
	}
	
	public String[] getStringArray( final Enum<?> key ) {
		return getStringArray( key.name().toLowerCase() );
	}
	
	public void setProperty( final Enum<?> key, Object arg ) {
		setProperty( key.name().toLowerCase(), arg );
	}
	
	public Configuration subset( final Enum<?> key ) {
		return subset( key.name().toLowerCase() );
	}

	public String toString() {
		return ConfigurationUtils.toString( this );
	}

	public int hashCode() {
		int h = 0;
		for( Iterator<?> i = getKeys(); i.hasNext(); ) h = h * 31 + Arrays.hashCode( getStringArray( (String)i.next() ) );
		return h;
	}

	/** Returns true if the provided object is equal to this set of properties.
	 * 
	 * <p>Equality between set of properties happens when the keys are the same,
	 * and the list of strings associated to each key is the same. Note that the order
	 * in which different keys appear in a property file is irrelevant, but <em>the order
	 * between properties with the same key is significant</em>.
	 *
	 * <p>Due to the strictness of the check (e.g., no number conversion is performed)
	 * this method is mainly useful when writing tests.
	 * 
	 * @return true if the argument is equal to this set of properties.
	 */
	
	public boolean equals( final Object o ) {
		if ( ! ( o instanceof Properties ) ) return false;
		final Properties p = (Properties)o;
		for( Iterator<?> i = getKeys(); i.hasNext(); ) {
			String key = (String)i.next();
			String[] value = p.getStringArray( key );
			if ( value == null || ! Arrays.equals( getStringArray( key ), value ) ) return false;
		}

		for( Iterator<?> i = p.getKeys(); i.hasNext(); )
			if ( getStringArray( (String)i.next() ) == null ) return false;

		return true;
	}
	
	private void writeObject( final ObjectOutputStream s ) throws IOException {
		s.defaultWriteObject();
		try {
			save( s );
		}
		catch ( ConfigurationException e ) {
			throw new RuntimeException( e );
		}
	}

	private void readObject( final ObjectInputStream s ) throws IOException, ClassNotFoundException {
		s.defaultReadObject();
		try {
			load( s );
		}
		catch ( ConfigurationException e ) {
			throw new RuntimeException( e );
		}
	}
}
