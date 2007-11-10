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
package com.bigdata.rdf.inf;

/**
 * Rules having a tail of the general form:
 * <pre>
 * ( ?u C1 C2 )
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AbstractRuleRdfs_6_8_10_12_13 extends AbstractRuleNestedSubquery {
    
    public AbstractRuleRdfs_6_8_10_12_13(Triple head, Triple body) {

        super( head, new Pred[] { body });

        /*
         * check the variable binding pattern on the tail.
         */
        
        assert super.body.length == 1;
        
        assert super.body[0].s.isVar();
        assert super.body[0].p.isConstant();
        assert super.body[0].o.isConstant();
        
    }
   
}
