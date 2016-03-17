package it.unimi.dsi.bits;

import it.unimi.dsi.bits.TransformationStrategies;
import junit.framework.TestCase;

public class Utf16TransformationStrategyTest extends TestCase {

	public void testGetLong() {
		String s = new String( new char[] { '\u0001', '\u0002' } );
		assertEquals( 48, TransformationStrategies.prefixFreeUtf16().toBitVector( s ).length() );
		assertEquals( 0x40008000L, TransformationStrategies.prefixFreeUtf16().toBitVector( s ).getLong( 0, 32 ) );
		assertEquals( 0x40008000L, TransformationStrategies.prefixFreeUtf16().toBitVector( s ).getLong( 0, 48 ) );
		s = new String( new char[] { '\u0001', '\u0002', '\u0003' } );
		assertEquals( 64, TransformationStrategies.prefixFreeUtf16().toBitVector( s ).length() );
		assertEquals( 0xC00040008000L, TransformationStrategies.prefixFreeUtf16().toBitVector( s ).getLong( 0, 48 ) );
		assertEquals( 0xC00040008000L, TransformationStrategies.prefixFreeUtf16().toBitVector( s ).getLong( 0, 64 ) );
		s = new String( new char[] { '\u0001', '\u0002', '\u0003', '\u0004' } );
		assertEquals( 80, TransformationStrategies.prefixFreeUtf16().toBitVector( s ).length() );
		assertEquals( 0x2000C00040008000L, TransformationStrategies.prefixFreeUtf16().toBitVector( s ).getLong( 0, 64 ) );
		assertEquals( 0, TransformationStrategies.prefixFreeUtf16().toBitVector( s ).getLong( 64, 80 ) );
		//System.err.println( Long.toHexString( TransformationStrategies.prefixFreeUtf16().toBitVector( s ).getLong( 16, 80 ) ));
		assertEquals( 0x2000C0004000L, TransformationStrategies.prefixFreeUtf16().toBitVector( s ).getLong( 16, 80 ) );

	
		s = new String( new char[] { '\u0001', '\u0002' } );
		assertEquals( 32, TransformationStrategies.utf16().toBitVector( s ).length() );
		assertEquals( 0x40008000L, TransformationStrategies.utf16().toBitVector( s ).getLong( 0, 32 ) );
		s = new String( new char[] { '\u0001', '\u0002', '\u0003' } );
		assertEquals( 48, TransformationStrategies.utf16().toBitVector( s ).length() );
		assertEquals( 0xC00040008000L, TransformationStrategies.utf16().toBitVector( s ).getLong( 0, 48 ) );
		s = new String( new char[] { '\u0001', '\u0002', '\u0003', '\u0004' } );
		assertEquals( 64, TransformationStrategies.utf16().toBitVector( s ).length() );
		assertEquals( 0x2000C00040008000L, TransformationStrategies.utf16().toBitVector( s ).getLong( 0, 64 ) );
	}
	
}
