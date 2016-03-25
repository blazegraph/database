package it.unimi.dsi.util;

/*		 
 * MG4J: Managing Gigabytes for Java
 *
 * Copyright (C) 2007-2009 Sebastiano Vigna 
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

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.util.SemiExternalGammaList;

import java.io.IOException;

import junit.framework.TestCase;

/**
 * @author Fabien Campagne
 * @author Sebastiano Vigna
 */
public class SemiExternalGammaListTest extends TestCase {

	private static InputBitStream buildInputStream( LongList longs ) throws IOException {
		byte[] array = new byte[ longs.size() * 4 ];
		OutputBitStream streamer = new OutputBitStream( array );
		for ( int i = 0; i < longs.size(); i++ ) streamer.writeLongGamma( longs.getLong( i ) );
		int size = (int)( streamer.writtenBits() / 8 ) + ( ( streamer.writtenBits() % 8 ) == 0 ? 0 : 1 );
		byte[] smaller = new byte[ size ];
		System.arraycopy( array, 0, smaller, 0, size );

		return new InputBitStream( smaller );

	}

	
    public void testSemiExternalGammaListGammaCoding() throws IOException {

		long[] longs = { 10, 300, 450, 650, 1000, 1290, 1699 };
		LongList listLongs = new LongArrayList( longs );

		SemiExternalGammaList list = new SemiExternalGammaList( buildInputStream( listLongs ), 1, listLongs.size() );
		for ( int i = 0; i < longs.length; ++i ) {
			assertEquals( ( "test failed for index: " + i ), longs[ i ], list.getLong( i ) );
		}

		list = new SemiExternalGammaList( buildInputStream( listLongs ), 2, listLongs.size() );
		for ( int i = 0; i < longs.length; ++i ) {
			assertEquals( ( "test failed for index: " + i ), longs[ i ], list.getLong( i ) );
		}

		list = new SemiExternalGammaList( buildInputStream( listLongs ), 4, listLongs.size() );
		for ( int i = 0; i < longs.length; ++i ) {
			assertEquals( ( "test failed for index: " + i ), longs[ i ], list.getLong( i ) );
		}

		list = new SemiExternalGammaList( buildInputStream( listLongs ), 7, listLongs.size() );
		for ( int i = 0; i < longs.length; ++i ) {
			assertEquals( ( "test failed for index: " + i ), longs[ i ], list.getLong( i ) );
		}
		
		list = new SemiExternalGammaList( buildInputStream( listLongs ), 8, listLongs.size() );
		for ( int i = 0; i < longs.length; ++i ) {
			assertEquals( ( "test failed for index: " + i ), longs[ i ], list.getLong( i ) );
		}
    }

    public void testEmptySemiExternalGammaListGammaCoding() throws IOException {

		long[] longs = {  };
		LongList listOffsets = new LongArrayList( longs );

		new SemiExternalGammaList( buildInputStream( listOffsets ), 1, listOffsets.size() );
		assertTrue( true );
    }

}
