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
 * Created July 10, 2010
 */

package com.bigdata.rdf.internal;

import java.io.Serializable;

/**
 * An object which describes which kinds of RDF Values are inlined into the
 * statement indices and how other RDF Values are coded into the lexicon.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME Version information in serialization format.
 * 
 * @todo large literal size boundary.
 * @todo other configuration options.
 */
public class LexiconConfiguration implements Serializable,
        ILexiconConfiguration {
    
    private static final long serialVersionUID = -817370708683316807L;
    
    private boolean inlineTerms;
    
    public LexiconConfiguration() {
    }
    
    public LexiconConfiguration(final boolean inlineTerms) {
        this.inlineTerms = inlineTerms;
    }
    
    /**
     * See {@link ILexiconConfiguration#isInline(DTE)}.
     */
    public boolean isInline(DTE dte) {

        return inlineTerms && isSupported(dte);
        
    }
    
    private boolean isSupported(DTE dte) {
        
        switch (dte) {
        case XSDBoolean:
        case XSDByte:
        case XSDShort:
        case XSDInt:
        case XSDLong:
        case XSDFloat:
        case XSDDouble:
        case XSDInteger:
        case UUID:
            return true;
        case XSDUnsignedByte:       // none of the unsigneds are tested yet
        case XSDUnsignedShort:      // none of the unsigneds are tested yet
        case XSDUnsignedInt:        // none of the unsigneds are tested yet
        case XSDUnsignedLong:       // none of the unsigneds are tested yet
        case XSDDecimal:            // need to implement byteLength() first
        default:
            return false;
        }
        
    }

    /**
     * See {@link ILexiconConfiguration#isLegacyEncoding()}.
    public boolean isLegacyEncoding() {
        
        return true;
        
    }
     */
    
}
