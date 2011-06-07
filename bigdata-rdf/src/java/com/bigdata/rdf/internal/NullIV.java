/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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
/*
 * Created on Jul 9, 2010
 */

package com.bigdata.rdf.internal;

/**
 * Class is used solely to encode and decode <code>null</code> {@link IV}
 * references. <code>null</code> {@link IV}s are somewhat special. They get used
 * as wildcards for the keys in the justifications index and perhaps (?) in a
 * few other locations.
 * 
 * @author mrpersonick
 * 
 * @deprecated by {@link TermId#NullIV}
 */
class NullIV extends TermId {

//    private static final long serialVersionUID = -214758033769962923L;
//    
//    final public static transient IV INSTANCE = new NullIV();
//
//	/**
//	 * Return <code>true</code> if the unsigned byte[] key represents a
//	 * {@link NullIV}.
//	 * 
//	 * @param key
//	 *            The key.
//	 *            
//	 * @return <code>true</code> if the key represents a {@link NullIV}.
//	 */
//	static boolean isNullIV(final byte[] key) {
//
//		for (int i = 0; i < key.length; i++) {
//
//			if (key[i] != 0)
//				return false;
//			
//		}
//
//		return true;
//    	
//    }
//
//    /**
//     * Note: This key is all (unsigned) zeros.
//     */
//    static private byte[] getNullIVKey() {
//
//    	final byte[] key = new byte[TermsIndexHelper.TERMS_INDEX_KEY_SIZE];
//    	
//    	key[0] = TermId.toFlags(VTE.URI); // Note: VTE.URI is unsigned ZERO.
//
//    	return key;
//    	
//    }
//    
    private NullIV() {
    	super(null);
//		super(getNullIVKey());
////        super(VTE.BNODE, TermId.NULL);
    }

}
