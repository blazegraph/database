/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Nov 16, 2011
 */

package com.bigdata.rdf.internal;

import java.util.Collection;
import java.util.LinkedList;

import com.bigdata.rdf.internal.impl.extensions.DateTimeExtension;
import com.bigdata.rdf.internal.impl.extensions.DerivedNumericsExtension;
import com.bigdata.rdf.internal.impl.extensions.USDFloatExtension;
import com.bigdata.rdf.internal.impl.extensions.XSDStringExtension;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataLiteral;

/**
 * Adds inlining for the
 * <code>http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/USD</code>
 * datatype, which is treated as <code>xsd:float</code>.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BSBMExtensionFactory implements IExtensionFactory {

    private final Collection<IExtension> extensions;
    
    private volatile IExtension[] extensionsArray;
    
    public BSBMExtensionFactory() {

        extensions = new LinkedList<IExtension>(); 
            
    }
    
    public void init(final LexiconRelation lex) {

        // Extension to inline "USD" datatypes.
        extensions.add(new USDFloatExtension<BigdataLiteral>(lex));
        
        /*
         * Always going to inline the derived numeric types.
         */
        extensions.add(new DerivedNumericsExtension<BigdataLiteral>(lex));
        
        if (lex.isInlineDateTimes()) {
            
            extensions.add(new DateTimeExtension<BigdataLiteral>(
                    lex, lex.getInlineDateTimesTimeZone()));
            
        }

        if (lex.getMaxInlineStringLength() > 0) {
            /*
             * Note: This extension is used for both literals and URIs. It MUST
             * be enabled when MAX_INLINE_TEXT_LENGTH is GT ZERO (0). Otherwise
             * we will not be able to inline either the local names or the full
             * text of URIs.
             */
            extensions.add(new XSDStringExtension<BigdataLiteral>(lex, lex
                    .getMaxInlineStringLength()));
        }

        extensionsArray = extensions.toArray(new IExtension[extensions.size()]);
        
    }
    
    public IExtension[] getExtensions() {
        
        return extensionsArray;
        
    }

}
