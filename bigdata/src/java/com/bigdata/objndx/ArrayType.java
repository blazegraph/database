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
enum ArrayType {
        BYTE(),
        CHAR(),
        SHORT(),
        INT(),
        LONG(),
        FLOAT(),
        DOUBLE(),
        OBJECT();
        private ArrayType(){}
        static public ArrayType getArrayType(Object ary) {
            if(ary == null ) throw new IllegalArgumentException("null");
            String className = ary.getClass().getName();
            if(className.charAt(0)!='[') {
                throw new IllegalArgumentException("not an array type");
            }
            char ch = className.charAt(1);
            switch(ch) {
//            boolean  Z  
//            byte  B  
            case 'B': return BYTE;
//            char  C  
            case 'C': return CHAR;
//            class or interface  Lclassname;
            case 'L': return OBJECT;
//            double  D  
            case 'D': return DOUBLE;
//            float  F  
            case 'F': return FLOAT;
//            int  I  
            case 'I': return INT;
//            long  J  
            case 'J': return LONG;
//            short  S  
            case 'S': return SHORT;
            }
            throw new UnsupportedOperationException(
                    "unsupported primitive array type: " + className);
        }
    }