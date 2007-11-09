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

package com.bigdata.rdf.model;

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
    Inferred((byte)2);

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
    
    static public StatementEnum deserialize(byte[] val) {
        if(val.length!=1) {
            throw new RuntimeException("Expecting one byte, not "+val.length);
        }
        switch(val[0]) {
        case 0: return Explicit;
        case 1: return Axiom;
        case 2: return Inferred;
        default: throw new RuntimeException("Unexpected byte: "+val[0]);
        }
        
    }

    public byte[] serialize() {

        return new byte[]{code};
        
    }
    
}
