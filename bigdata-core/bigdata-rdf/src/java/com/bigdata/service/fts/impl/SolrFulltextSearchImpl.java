/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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

import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.FutureResponseListener;

import com.bigdata.service.fts.FTS;
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
public class SolrFulltextSearchImpl implements
      IFulltextSearch<FulltextSearchHit> {

   final private static transient Logger log = Logger
         .getLogger(SolrFulltextSearchImpl.class);

   @Override
   public FulltextSearchHiterator<FulltextSearchHit> search(
         com.bigdata.service.fts.IFulltextSearch.FulltextSearchQuery query,
         HttpClient client) {

      if (query != null) {

         try {
            FulltextSearchHit[] hits = queryIndex(query, client);

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
   private FulltextSearchHit[] queryIndex(
         FulltextSearchQuery query, HttpClient httpClient)
         throws Exception {

      if (httpClient.isStopped()) {
         throw new FulltextSearchException("The client has been stopped");
      }
      
      
      Request request = httpClient.newRequest(query.getEndpoint());
      
      // Limit response content buffer to 512 KiB
      FutureResponseListener listener =
         new FutureResponseListener(request, 10 * 1024 * 1024); // 100 MB size
      
      request.param("q", query.getQuery());
      request.param("wt", "json");

      final String searchParams = query.getParams();
      if (searchParams!=null && !searchParams.isEmpty()) {
         final String[] params = searchParams.split("&");
         for (int i=0; i<params.length; i++) {
            if (params[i]!=null) {
               String kv[] = params[i].split("=");
               if (kv.length==2 && kv[0]!=null && !(kv[0].isEmpty())) {
                  if (!(kv[0].equals("wt"))) {
                     try {
                        final String val = kv[1]==null ? "" : 
                           URLDecoder.decode(kv[1], "UTF-8");
                        request.param(kv[0], val);
                     } catch (Exception e) {
                        if (log.isInfoEnabled()) {
                           log.info("Solr search param: '" + params[i] + "'" +
                                 "' can't be URL decoded. Will be ignored...");
                        }
                     }
                  }
               } else {
                  if (log.isInfoEnabled()) {
                     log.info("Invalid Solr search param: '" + params[i] + "'");
                     log.info("Will be ignored...");
                  }
               }
            }
         }
      }
      
      final Integer queryTimeoutSpecified = query.getSearchTimeout();
      final Integer queryTimeoutUsed = 
            queryTimeoutSpecified==null ?
            FTS.Options.DEFAULT_TIMEOUT : queryTimeoutSpecified;
      
      request.send(listener);
      ContentResponse resp = listener.get(queryTimeoutUsed, TimeUnit.MILLISECONDS);

      final int statusCode = resp.getStatus();
      if (statusCode != 200) {

         throw new FulltextSearchException("Status code != 200 received from "
               + "external fulltext service: " + statusCode);

      }

      final String jsonStr = resp.getContentAsString();
      final JSONObject json = new JSONObject(jsonStr);

      return constructFulltextSearchList(json, query);

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
