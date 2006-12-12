/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Dec 9, 2006
 */

package com.bigdata.objndx;

/**
 * Enumeration identifies whether the keys are an array of some primitive data
 * type or an array of instances of some Class.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public enum ArrayType {
        
    BYTE(1), CHAR(2), SHORT(3), INT(4), LONG(5), FLOAT(6), DOUBLE(7),
    
    OBJECT(10);

    private final int id;

    private ArrayType(int id) {

        this.id = id;

    }

    /**
     * Return an external identifier for this enum value than may be serialized,
     */
    public int intValue() {

        return id;

    }

    /**
     * Parse an external identifier returning the corresponding enum value.
     * 
     * @param id
     *            The external identifier.
     * 
     * @return The enum value.
     * 
     * @exception IllegalArgumentException
     *                if the identifier does not correspond to a recognized enum
     *                value.
     */
    static public ArrayType parseInt(int id) {

        switch (id) {
        case 1:
            return BYTE;
        case 2:
            return CHAR;
        case 3:
            return SHORT;
        case 4:
            return INT;
        case 5:
            return LONG;
        case 6:
            return FLOAT;
        case 7:
            return DOUBLE;
        case 10:
            return OBJECT;
        default:
            throw new IllegalArgumentException("Not a known identifier: id="
                    + id);
        }

    }

    /**
     * Return the enum value for the array type.
     * 
     * @param ary
     *            The array.
     *            
     * @return The array type.
     */
    static public ArrayType getArrayType(Object ary) {
        if (ary == null)
            throw new IllegalArgumentException("null");
        String className = ary.getClass().getName();
        if (className.charAt(0) != '[') {
            throw new IllegalArgumentException("not an array type");
        }
        char ch = className.charAt(1);
        switch (ch) {
        //            boolean  Z  
        case 'B':
            return BYTE;
        case 'C':
            return CHAR;
        case 'L':
            return OBJECT;
        case 'D':
            return DOUBLE;
        case 'F':
            return FLOAT;
        case 'I':
            return INT;
        case 'J':
            return LONG;
        case 'S':
            return SHORT;
        }
        throw new UnsupportedOperationException(
                "unsupported primitive array type: " + className);
    }

    /**
     * Return a human readable label for the enum value ("byte", "short", etc).
     */
    public String toString() {
        switch (this) {
        case BYTE:
            return "byte";
        case SHORT:
            return "short";
        case CHAR:
            return "char";
        case INT:
            return "int";
        case LONG:
            return "long";
        case FLOAT:
            return "float";
        case DOUBLE:
            return "double";
        case OBJECT:
            return "Object";
        default:
            throw new UnsupportedOperationException();
        }
    }

}
