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
package com.bigdata.rdf.internal;

/**
 * 
 * Abstract class that defines the integer packing needed to allow for 
 * support of multiple inline integer URI handlers with the same namepace.
 * 
 * This is required as once the integer value is inlined it is not possible
 * to determine which handler to apply if there are multiple namespaces that use
 * different prefixed or suffixed inline handlers.  
 * 
 * For example:
 * <code> 
 *   http://rdf.ncbi.nlm.nih.gov/pubchem/descriptor/ 
 *   descriptor:CID5606223_Compound_Identifier , 
 *   descriptor:CID5606223_Covalent_Unit_Count , 
 *   descriptor:CID5606223_Defined_Atom_Stereo_Count ,
 * </code>  
 * 
 *   Each of these can be supported as a different {@link InlinePrefixedSuffixedIntegerURIHandler} at the
 *   namespace http://rdf.ncbi.nlm.nih.gov/pubchem/descriptor/.
 *   
 *   It current supports up to 32 different inline values for a single namespace URI.  It is used
 *   in conjunction with the {@link InlineIntegerURIHandlerMap}.
 *   
 *   For an example, see {@see com.blazegraph.vocab.pubchem.PubchemInlineURIFactory}.
 *   
 *   The max inline integer value that can supported in this scheme is 2^57 - 1, which is 2^5 less
 *   than the Java {@link Long.MAX_VALUE}.  
 * 
 * @author beebs
 *
 */
public abstract class InlineLocalNameIntegerURIHandler extends
		InlineSignedIntegerURIHandler {
	
	/**
	 * The ID to pack.  From 0 to the MAX_IDS
	 */
	protected int packedId = 0;
	
	/**
	 * Maximum nubmer of IDs 
	 * 
	 * Up to 32 different values for a single URI.
	 */
	private static final int MAX_IDS = 32 - 1;
	
	/**
	 * 
	 * The bitmask used.
	 * 
	 */
	private static final long ID_MASK = 32L << 58;
	
	/**
	 *
	 * The maximum int value supported with packing.
	 * 2^57 -1
	 * 
	 */
	private static final long MAX_VALUE = 144115188075856000L;

	public InlineLocalNameIntegerURIHandler(String namespace) {
		super(namespace);
	}
	
	public InlineLocalNameIntegerURIHandler(String namespace, int packedId) {
		super(namespace);

		if(packedId > MAX_IDS) {
			throw new RuntimeException (packedId + " is greater than the max ids value:  " + MAX_IDS);
		}
		
		if(packedId < 0) {
			throw new RuntimeException (packedId + " is a negative value.");
		}

		this.packedId = packedId;
	}
	
	/**
	 * Pack the value with the leading 5 bits used to encode the 
	 * id used for decoding the inline URI handler. 
	 * 
	 * @param value
	 * @return
	 */
	public long packValue(final long value) {
		
		if(value >= MAX_VALUE) {
			throw new RuntimeException(value + " exceeds " + MAX_VALUE + ".");
		} else if (value < 0 ) {
			throw new RuntimeException(value + " is less than zero.");
		}
		
		long idVal = (long) packedId << 58;

		long packedVal = idVal | value;
		
		return packedVal;
			
	}
	
	/**
	 * Convenience method to take an Integer string value, pack it, and convert back to a string. 
	 * 
	 * @param intVal
	 * @return
	 */
	public String getPackedValueString(final String intVal) {
	
		String longVal = null;
		
		try {
			longVal = Long.toString(packValue((long)Long.parseLong(intVal)));
		} catch (NumberFormatException nfe) {
			//It is not a valid number
			//Do not inline
			return null;
		}
		
		return longVal;

	}
	
	/**
	 * Convenience method for unpacking a value from a string. 
	 * 
	 * @param s
	 * @return
	 */
	public String getUnpackedValueString(final String s) {
		return Long.toString(unpackValue((long)Long.parseLong(s)));
	}
	
	/**
	 * Convenience method to unpack a long value from a string. 
	 * 
	 * @param s
	 * @return
	 */
	public long getUnpackedValueFromString(final String s) {
		return unpackValue((long)Long.parseLong(s));
	}
	
	/**
	 * Take a packed long and extract the value. 
	 * 
	 * @param packedVal
	 * @return
	 */
	public long unpackValue(final long packedVal) {

		
		long idVal = (long) packedId << 58;

		long unpackedVal = packedVal  & ~idVal;
		
		return unpackedVal;

	}

	/**
	 * Convenience method to get the packed ID value from an
	 * input {@link String}. 
	 * 
	 * @param s
	 * @return
	 */
	public int unpackId(final String s) {
		return unpackId(Long.parseLong(s));
	}
	
	/**
	 * Take a packed long and extract the ID. 
	 * 
	 * @param packedVal
	 * @return
	 */
	public int unpackId(final long packedVal) {
		
		long unpackedId = (packedVal & ~ID_MASK) >> 58;
		
		return (int) unpackedId;
		
	}


}
