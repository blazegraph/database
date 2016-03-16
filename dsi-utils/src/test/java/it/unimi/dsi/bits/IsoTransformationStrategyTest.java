package it.unimi.dsi.bits;

import it.unimi.dsi.bits.TransformationStrategies;
import junit.framework.TestCase;

public class IsoTransformationStrategyTest extends TestCase {

	public void testGetLong() {
		String s = new String( new char[] { '\u0001', '\u0002' } );
		assertEquals( 24, TransformationStrategies.prefixFreeIso().toBitVector( s ).length() );
		assertEquals( 0x4080L, TransformationStrategies.prefixFreeIso().toBitVector( s ).getLong( 0, 16 ) );
		assertEquals( 0x4080L, TransformationStrategies.prefixFreeIso().toBitVector( s ).getLong( 0, 24 ) );
		s = new String( new char[] { '\u0001', '\u0002', '\u0003' } );
		assertEquals( 32, TransformationStrategies.prefixFreeIso().toBitVector( s ).length() );
		assertEquals( 0xC04080L, TransformationStrategies.prefixFreeIso().toBitVector( s ).getLong( 0, 24 ) );
		assertEquals( 0xC04080L, TransformationStrategies.prefixFreeIso().toBitVector( s ).getLong( 0, 32 ) );
		s = new String( new char[] { '\u0001', '\u0002', '\u0003', '\u0004' } );
		assertEquals( 40, TransformationStrategies.prefixFreeIso().toBitVector( s ).length() );
		assertEquals( 0x20C04080L, TransformationStrategies.prefixFreeIso().toBitVector( s ).getLong( 0, 32 ) );
		assertEquals( 0, TransformationStrategies.prefixFreeIso().toBitVector( s ).getLong( 32, 40 ) );
		//System.err.println( Long.toHexString( TransformationStrategies.prefixFreeIso().toBitVector( s ).getLong( 16, 80 ) ));
		assertEquals( 0x20C040L, TransformationStrategies.prefixFreeIso().toBitVector( s ).getLong( 8, 40 ) );

		s = new String( new char[] { '\u0001', '\u0002' } );
		assertEquals( 16, TransformationStrategies.iso().toBitVector( s ).length() );
		assertEquals( 0x4080L, TransformationStrategies.iso().toBitVector( s ).getLong( 0, 16 ) );
		s = new String( new char[] { '\u0001', '\u0002', '\u0003' } );
		assertEquals( 24, TransformationStrategies.iso().toBitVector( s ).length() );
		assertEquals( 0xC04080L, TransformationStrategies.iso().toBitVector( s ).getLong( 0, 24 ) );
		s = new String( new char[] { '\u0001', '\u0002', '\u0003', '\u0004' } );
		assertEquals( 32, TransformationStrategies.iso().toBitVector( s ).length() );
		assertEquals( 0x20C04080L, TransformationStrategies.iso().toBitVector( s ).getLong( 0, 32 ) );

		s = new String( new char[] { '\u0001', '\u00FF', '\u00FF', '\u00FF', '\u0001', '\u0001', '\u0001', '\u0001' } );
		assertEquals( 64, TransformationStrategies.iso().toBitVector( s ).length() );
		assertEquals( 0x80808080FFFFFF80L, TransformationStrategies.iso().toBitVector( s ).getLong( 0, 64 ) );
	}
	
}
