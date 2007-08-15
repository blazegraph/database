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
 * Created on Aug 15, 2007
 */

package com.bigdata.sparse;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class validates column and schema name constraints. 
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class NameChecker {

    /**
     * The constraint on schema and column names.
     */
    public final static Pattern pattern_name = Pattern.compile("[\\w._/]+");
    
    /**
     * Assert that the string is valid as the name of a schema. Names must be
     * alphanumeric and may also include any of {<code>.</code>,
     * <code>_</code>, or <code>/</code>}.
     * 
     * @param s
     *            The string.
     * 
     * @throws IllegalArgumentException
     *             if the string is not valid as the name of a schema.
     */
    static public void assertSchemaName(String s)
            throws IllegalArgumentException {

        if(s==null) throw new IllegalArgumentException();
        
        if(s.length()==0) throw new IllegalArgumentException();
        
        if(s.indexOf('\0')!=-1) throw new IllegalArgumentException(); 

        Matcher m = pattern_name.matcher(s);
        
        if(!m.matches()) throw new IllegalArgumentException();
        
    }

    /**
     * Assert that the string is valid as the name of a column. Names must be
     * alphanumeric and may also include any of {<code>.</code>,
     * <code>_</code>, or <code>/</code>}.
     * 
     * @param s
     *            The string.
     * 
     * @throws IllegalArgumentException
     *             if the string is not valid as the name of a column.
     */
    static public void assertColumnName(String s) 
            throws IllegalArgumentException {

        if(s==null) throw new IllegalArgumentException();

        if(s.length()==0) throw new IllegalArgumentException();

        if(s.indexOf('\0')!=-1) throw new IllegalArgumentException(); 
        
        Matcher m = pattern_name.matcher(s);
        
        if(!m.matches()) throw new IllegalArgumentException();

    }

}
