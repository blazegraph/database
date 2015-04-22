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

import java.io.Serializable;

import com.bigdata.search.Hiterator;
import com.bigdata.search.IHit;
import com.bigdata.search.IFulltextSearchHit;
import com.bigdata.search.FulltextSearchHiterator;

/**
 * Abstraction for search interface against external Solr index.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public interface IFulltextSearch<A extends IFulltextSearchHit> {
   
	/**
	 * Submit a seach query against the Solr Index
	 * 
	 * @param query
	 *            The query.
	 * 
	 * @return The result set.
	 */
    public FulltextSearchHiterator<A> search(final SolrSearchQuery query);

//    public int count(final ExternalSolrSearchQuery query);

    
    public static class SolrSearchQuery implements Serializable {

      private static final long serialVersionUID = -2509557655519603130L;
      
      final String query;
		final String params;
		final String endpoint;

		/**
		 * Constructor 
		 * 
		 * @param query the query string
		 * @param params the parameters to be passed to Solr
		 * @param endpoint the endpoint to which to submit the query
		 */
      public SolrSearchQuery(
         final String query, final String params, final String endpoint) {
         
         // TODO sanity check, e.g. endpoint and query must be set
         this.query = query==null ? "" : query;
         this.params = params==null ? "" : params;
         this.endpoint = endpoint==null ? "" : endpoint;
         
      }

		
		/**
		 * @return the query
		 */
		public String getQuery() {
			return query;
		}

	    /**
       * @return the query endpoint
       */
      public String getParams() {
         return params;
      }

		/**
		 * @return the query endpoint
		 */
      public String getEndpoint() {
         return endpoint;
      }

		/* (non-Javadoc)
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {
			final int prime = 31;
			
			int result = 1;
         result = prime * result + ((query == null) ? 0 : query.hashCode());
         result = prime * result + ((params == null) ? 0 : params.hashCode());
         result = prime * result + ((endpoint == null) ? 0 : endpoint.hashCode());
         
			return result;
		}

		/* (non-Javadoc)
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
    	@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			SolrSearchQuery other = (SolrSearchQuery) obj;
			
         if (!query.equals(other.query))
            return false;
         
         if (!params.equals(other.params))
            return false;
         
         if (!endpoint.equals(other.endpoint))
            return false;
         
			return true;
		}

   }

}
