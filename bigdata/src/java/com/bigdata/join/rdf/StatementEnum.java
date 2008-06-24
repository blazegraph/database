/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Oct 19, 2007
 */

package com.bigdata.join.rdf;

/**
 * The basic statement types are: axioms, explicit, inferred.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public enum StatementEnum {

    /**
     * A statement that was inserted into the database explicitly by the
     * application.
     */
    Explicit((byte)0),
    /**
     * Something that is directly entailed by the appropriate model theory.
     */
    Axiom((byte)1),
    /**
     * A statement that was inferred from the explicit statements by the
     * appropriate model theory.
     */
    Inferred((byte)2),
    /**
	 * For debugging.
 	 */
    Backchained((byte)4);

    private final byte code;
    
    private StatementEnum(byte code) {
     
        this.code = code;
        
    }

    public byte code() {
        
        return code;
        
    }
    
    /**
     * Max returns the value that is first in the total order
     * <ul>
     * <li>Explicit</li>
     * <li>Axiom</li>
     * <li>Inferred</li>
     * </ul>
     * @param a
     * @param b
     * @return
     */
    static public StatementEnum max(StatementEnum a, StatementEnum b) {
        
        if (a.code < b.code) {
        
            return a;
        
        } else {
        
            return b; 
        
        }
        
    }

    /**
     * Decode a byte into a {@link StatementEnum}.
     * <p>
     * Note: The override bit is masked off during this operation.
     * 
     * @param b
     *            The byte.
     *            
     * @return The {@link StatementEnum} value.
     */
    static public StatementEnum decode(byte b) {

        switch (b & ~MASK_OVERRIDE) {

        case 0: return Explicit;
        
        case 1: return Axiom;
        
        case 2: return Inferred;
        
        case 4: return Backchained;
        
        default:
            throw new RuntimeException("Unexpected byte: " + b);
        
        }

    }
    
//    static public StatementEnum deserialize(DataInputBuffer in) {
//        
//        try {
//
//            return decode(in.readByte());
//            
//        } catch(IOException ex) {
//            
//            throw new UnsupportedOperationException();
//            
//        }
//        
//    }

    static public StatementEnum deserialize(byte[] val) {

        if (val.length != 1 && val.length != (1 + 8)) {

            throw new RuntimeException(
                    "Expecting either one byte or nine bytes, not "
                            + val.length);
            
        }
        
        return decode(val[0]);
        
    }

    public byte[] serialize() {

        return new byte[]{code};
        
    }

    /**
     * A bit mask used to isolate the bit that indicates that the existing
     * statement type should be overriden thereby allowing the downgrade of a
     * statement from explicit to inferred.
     */
    public static final int MASK_OVERRIDE = 0x1<<3;

    /**
     * Return <code>true</code> iff the override bit is set.
     * 
     * @param b
     *            The byte.
     */
    public static boolean isOverride(byte b) {
        
        return (b & StatementEnum.MASK_OVERRIDE) == 1;
        
    }
    
}
