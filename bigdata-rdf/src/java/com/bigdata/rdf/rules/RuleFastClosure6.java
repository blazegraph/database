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
 * Created on Oct 25, 2007
 */

package com.bigdata.rdf.rules;


public class RuleFastClosure6 extends AbstractRuleFastClosure_5_6_7_9 {

    /**
     * 
     */
    private static final long serialVersionUID = 2335860655936029008L;

    /**
     * @param inf
     * @param R
     */
    public RuleFastClosure6(String database, String focusStore, RDFSVocabulary inf) {
        // Set<Long> R) {

        super("fastClosure6", database, focusStore, inf.rdfsSubPropertyOf,
                inf.rdfsRange);// , R);

    }
    
}