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
package com.bigdata.service.fts.impl;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.bigdata.service.fts.FulltextSearchException;
import com.bigdata.service.fts.FulltextSearchHit;
import com.bigdata.service.fts.FulltextSearchHiterator;
import com.bigdata.service.fts.IFulltextSearch;

/**
 * Implementation based on the built-in keyword search capabilities for bigdata.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
// TODO: builtin exceptions
public class SolrFulltextSearchImpl implements
      IFulltextSearch<FulltextSearchHit> {

   final private static transient Logger log = Logger
         .getLogger(SolrFulltextSearchImpl.class);

   @Override
   public FulltextSearchHiterator<FulltextSearchHit> search(
         com.bigdata.service.fts.IFulltextSearch.FulltextSearchQuery query) {

      if (query != null) {

         try {
            FulltextSearchHit[] hits = queryIndex(query);

            return new FulltextSearchHiterator<FulltextSearchHit>(hits);

         } catch (Exception e) {

            throw new FulltextSearchException(
                  "Error execution fulltext search: " + e);
         }
      }

      return new FulltextSearchHiterator<FulltextSearchHit>(
            new FulltextSearchHit[] {});

   }

   
   @SuppressWarnings("deprecation")
   private FulltextSearchHit[] queryIndex(FulltextSearchQuery query)
         throws Exception {

      final HttpClient httpClient;
      final Integer queryTimeout = query.getSearchTimeout();
      if (queryTimeout!=null) {
         
         final HttpParams httpParams = new BasicHttpParams();
         HttpConnectionParams.setConnectionTimeout(httpParams, queryTimeout);
         HttpConnectionParams.setSoTimeout(httpParams, queryTimeout);
         httpClient = new DefaultHttpClient(httpParams);
         
      } else {
         
         httpClient = new DefaultHttpClient();
         
      }

      try {

         final StringBuffer requestStr = new StringBuffer();
         requestStr.append(query.getEndpoint());
         requestStr.append("?q=" + URLEncoder.encode(query.getQuery(),"UTF-8"));
         requestStr.append("&wt=json");
         
         final String searchParams = query.getParams();
         if (searchParams!=null && !searchParams.isEmpty()) {
            requestStr.append("&");
            requestStr.append(searchParams);
         }

         final HttpGet httpGet = new HttpGet(requestStr.toString());
         
         final HttpResponse response = httpClient.execute(httpGet);
         

         if (response == null) {

            throw new FulltextSearchException(
                  "No response from fulltext service");

         }

         final int statusCode = response.getStatusLine().getStatusCode();
         if (statusCode != 200) {

            throw new FulltextSearchException(
                  "Status code != 200 received from "
                        + "external fulltext service: " + statusCode);

         }

         final String jsonStr = EntityUtils.toString(response.getEntity(), "UTF-8");
         final JSONObject json = new JSONObject(jsonStr);

         return constructFulltextSearchList(json, query);

      } catch (IOException e) {

         throw new FulltextSearchException(
               "Error submitting the keyword search"
                     + " to the external service: " + e.getMessage());

      } finally {

         if (httpClient!=null) {
            httpClient.getConnectionManager().shutdown();
         }
         
      }

   }

   /**
    * Constructs a list of fulltext search results from a Solr json result
    * string.
    * 
    * @param solrResultsJSON
    * @param query
    * @return
    * @throws JSONException
    */
   private FulltextSearchHit[] constructFulltextSearchList(
         JSONObject solrResultsJSON, FulltextSearchQuery query)
         throws JSONException {

      String searchColumn = query.getSearchField();
      String snippetColumn = query.getSnippetField();
      String scoreColumn = query.getScoreField();
      
      JSONObject resp = solrResultsJSON.getJSONObject("response");
      JSONArray docs = resp.getJSONArray("docs");

      /**
       * Collect results from JSON
       */
      List<FulltextSearchHit> searchHits =
            new ArrayList<FulltextSearchHit>(docs.length());
      for (int i = 0; i < docs.length(); i++) {

         JSONObject result = docs.getJSONObject(i);

         String search = null;
         if (searchColumn!=null && !searchColumn.isEmpty()
               && result.has(searchColumn)) {
            
            search = flattenJsonResult(result.get(searchColumn));
                  
         } else {
            
            throw new FulltextSearchException(
                  "Search field undefined, empty, or does not exist.");

         }

               
         String snippet = null;
         if (snippetColumn!=null && !snippetColumn.isEmpty()) {
            
            snippet = result.has(snippetColumn) ?
                  flattenJsonResult(result.get(snippetColumn)) : null;
            
         }
          
         String score = null;
         if (scoreColumn!=null && !scoreColumn.isEmpty()) {

            score = result.has(scoreColumn) ?
                  flattenJsonResult(result.get(scoreColumn)) : null;
            
         }
            
         Double scoreAsDouble = null;
         if (score!=null) {

            try {
               scoreAsDouble = Double.valueOf(score);
            } catch (NumberFormatException e) {
               
               if (log.isInfoEnabled()) {
                  log.info("Could not cast score to double: " + score);
               }
            }
            
         }

         if (search!=null && !search.isEmpty()) {
            FulltextSearchHit currentHit = 
                  new FulltextSearchHit(search, scoreAsDouble, snippet,
                  query.getIncomingBindings(), query.getSearchResultType());

            searchHits.add(currentHit);
            
         }

      }

      return searchHits.toArray(new FulltextSearchHit[searchHits.size()]);
   }
   
   /**
    * Flattens a JSON result item, i.e. if the item is an array, it is
    * (non-recursively) flattened, applying toString() to sub items,
    * otherwise toString() is called directly.
    * 
    * @param obj the json result item
    * @return
    */
   String flattenJsonResult(Object obj) {
      
      if (obj instanceof JSONArray) {
         
         StringBuffer buf = new StringBuffer();
         
         final JSONArray arr = (JSONArray)obj;
         for (int i=0; i<arr.length(); i++) {
            
            try {
               
               final Object cur = arr.get(i);
               
               if (cur!=null) {
                  buf.append(cur.toString());
                  
               }
               
            } catch (Exception e) {

               // ignoring is probably the best we can do here
               
            }
         }
         
         return buf.toString();
         
      } else {
         
         return obj.toString();
         
      }
      
   }

}
