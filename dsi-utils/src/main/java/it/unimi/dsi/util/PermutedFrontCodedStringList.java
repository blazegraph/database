package it.unimi.dsi.util;

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

import it.unimi.dsi.fastutil.ints.IntIterators;
import it.unimi.dsi.fastutil.ints.IntListIterator;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.AbstractObjectList;
import it.unimi.dsi.fastutil.objects.AbstractObjectListIterator;
import it.unimi.dsi.fastutil.objects.ObjectListIterator;
import it.unimi.dsi.lang.MutableString;

import java.io.IOException;
import java.io.Serializable;

import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

/** A {@link it.unimi.dsi.util.FrontCodedStringList} whose indices are permuted.
 * 
 * <P>It may happen that a list of strings compresses very well 
 * using front coding, but unfortunately alphabetical order is <em>not</em>
 * the right order for the strings in the list. Instances of this class
 * wrap an instance of {@link it.unimi.dsi.util.FrontCodedStringList}
 * together with a permutation &pi;: inquiries with index <var>i</var> will
 * actually return the string with index &pi;<sub><var>i</var></sub>.
 */

public class PermutedFrontCodedStringList extends AbstractObjectList<CharSequence> implements Serializable {

	public static final long serialVersionUID = -7046029254386353130L;

	/** The underlying front-coded string list. */
	final protected FrontCodedStringList frontCodedStringList;
	/** The permutation. */
	final protected int[] permutation;

	/** Creates a new permuted front-coded string list using a given front-coded string list and permutation.
	 * 
	 * @param frontCodedStringList the underlying front-coded string list.
	 * @param permutation the underlying permutation.
	 */

	public PermutedFrontCodedStringList( final FrontCodedStringList frontCodedStringList, final int[] permutation ) {
		this.frontCodedStringList = frontCodedStringList;
		this.permutation = permutation;
	}
	
	public CharSequence get( final int index ) { 
		return frontCodedStringList.get( permutation[ index ] );
	}

	/** Returns the element at the specified position in this front-coded list by storing it in a mutable string.
	 *
	 * @param index an index in the list.
	 * @param s a mutable string that will contain the string at the specified position.
	 */
	public void get( final int index, final MutableString s ) { 
		frontCodedStringList.get( permutation[ index ], s );
	}

	public int size() {
		return frontCodedStringList.size();
	}

	public ObjectListIterator<CharSequence> listIterator( final int k ) { return new AbstractObjectListIterator<CharSequence>() {
			final IntListIterator i = IntIterators.fromTo( 0, frontCodedStringList.size() );
			
			public boolean hasNext() { return i.hasNext(); }
			public boolean hasPrevious() { return i.hasPrevious(); }
			public CharSequence next() { return frontCodedStringList.get( permutation[ i.nextInt() ] ); }
			public CharSequence previous() { return frontCodedStringList.get( permutation[ i.previousInt() ] ); }
			public int nextIndex() { return i.nextIndex(); }
			public int previousIndex() { return i.previousIndex(); }
		};
	}

	public static void main( final String[] arg ) throws IOException, ClassNotFoundException, JSAPException {

		SimpleJSAP jsap = new SimpleJSAP( PermutedFrontCodedStringList.class.getName(), "Builds a permuted front-coded list of strings using a given front-coded string list and permutation",  
				new Parameter[] {
					new Switch( "invert", 'i', "invert", "Invert permutation before creating the permuted list." ),
					new UnflaggedOption( "list", JSAP.STRING_PARSER, JSAP.REQUIRED, "A front-coded string list." ),
					new UnflaggedOption( "permutation", JSAP.STRING_PARSER, JSAP.REQUIRED, "A permutation for the indices of the list." ),
					new UnflaggedOption( "permutedList", JSAP.STRING_PARSER, JSAP.REQUIRED, "A the filename for the resulting permuted list." ),
			} );

		JSAPResult jsapResult = jsap.parse( arg ); 
		if ( jsap.messagePrinted() ) return;
		
		int[] basePermutation =  BinIO.loadInts( jsapResult.getString( "permutation" ) ), permutation;
		if ( jsapResult.getBoolean( "invert" ) ) {
			int i = basePermutation.length;
			permutation = new int[ i ];
			while( i-- != 0 ) permutation[ basePermutation[ i ] ] = i;
		}
		else permutation = basePermutation;
		
		basePermutation = null;
		
		BinIO.storeObject( 
				new PermutedFrontCodedStringList( (FrontCodedStringList)BinIO.loadObject( jsapResult.getString( "list" ) ), permutation ), 
				jsapResult.getString( "permutedList" ) 
		);
	}
}
