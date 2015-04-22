/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * Created on Jun 3, 2010
 */

package com.bigdata.rdf.lexicon;

import org.apache.log4j.Logger;

import com.bigdata.search.FulltextSearchHit;
import com.bigdata.search.FulltextSearchHiterator;

/**
 * Implementation based on the built-in keyword search capabilities for bigdata.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class SolrFulltextSearchImpl 
implements IFulltextSearch<FulltextSearchHit> {

    final private static transient Logger log = Logger
            .getLogger(SolrFulltextSearchImpl.class);

    
    public SolrFulltextSearchImpl() {

    }


   @Override
   public FulltextSearchHiterator<FulltextSearchHit> search(
         com.bigdata.rdf.lexicon.IFulltextSearch.SolrSearchQuery query) {
      // TODO: query against Solr endpoint goes here
      FulltextSearchHit hit1 = new FulltextSearchHit("http://example.com/s1", 1.0, "SNIPPET 1");
      FulltextSearchHit hit2 = new FulltextSearchHit("http://example.com/s2", 0.9, "SNIPPET 2");
      
      FulltextSearchHit[] hits = { hit1, hit2 };
      return new FulltextSearchHiterator<FulltextSearchHit>(hits);
   }

//   @Override
//   public int count(
//         com.bigdata.rdf.lexicon.ISolrSearch.ExternalSolrSearchQuery query) {
//      // TODO Auto-generated method stub
//      return 0;
//   }


}
