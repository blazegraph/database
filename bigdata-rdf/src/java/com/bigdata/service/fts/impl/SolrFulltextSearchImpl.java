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
import org.apache.http.params.HttpParams;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.bigdata.service.fts.FulltextSearchHit;
import com.bigdata.service.fts.FulltextSearchHiterator;
import com.bigdata.service.fts.IFulltextSearch;
import com.bigdata.service.fts.IFulltextSearch.FulltextSearchQuery;

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

      // TODO: remove for production:
      FulltextSearchHit hit1 = new FulltextSearchHit(
            "http://example.com/s1", 1.0, "SNIPPET 1", query.getIncomingBindings(), query.getTargetType());
      FulltextSearchHit hit2 = new FulltextSearchHit(
            "http://example.com/s2", 0.9, "SNIPPET 2", query.getIncomingBindings(), query.getTargetType());
      
      FulltextSearchHit[] hits = { hit1, hit2 };
      return new FulltextSearchHiterator<FulltextSearchHit>(hits);

      /*
      if (query != null) {

         try {
            FulltextSearchHit[] hits = queryIndex(query);

            return new FulltextSearchHiterator<FulltextSearchHit>(hits);

         } catch (Exception e) {

            throw new RuntimeException("Error execution fulltext search: " + e);
         }
      }

      return new FulltextSearchHiterator<FulltextSearchHit>(
            new FulltextSearchHit[] {});
            */
   }

   private FulltextSearchHit[] queryIndex(FulltextSearchQuery query)
         throws Exception {

      final HttpClient httpClient = new DefaultHttpClient();
      final HttpGet request = new HttpGet(query.getEndpoint());

      // TODO: remove this example once we verified everything works fine
      // String solrIndexURL =
      // "http://128.86.231.88:8983/solr/select/"
      // +
      // "?q=Rembrandt*+AND+%28+thesaurus%3Ahttp%3A%2F%2Fcollection.britishmuseum.org%2Fid%2Fperson-institution+%29+&"
      // + "fl=id%2Clabel%2Cnotes%2Cthesaurus&indent=on"
      // +
      // "&bf=ln%28uses%29&qf=label_str%5E16+label%5E8+altLabel_str%5E4+altLabel%5E2+text"
      // + "&wt=json&rows=50&defType=edismax";

      final HttpParams params = new BasicHttpParams();
      request.setParams(params);

      // the search query is passed as a parameter
      params.setParameter("q", URLEncoder.encode(query.getQuery(), "UTF8"));

      // and also the special param string needs to be passed
      String searchParams = query.getParams();
      if (searchParams != null) {
         final String[] searchParamsSpl = searchParams.split("&");
         for (int i = 0; i < searchParamsSpl.length; i++) {

            final String searchParam = searchParamsSpl[i];
            final String[] keyValue = searchParam.split("=");
            if (keyValue.length == 2) {
               params.setParameter(keyValue[0], keyValue[1]);
            }
         }
      }

      try {

         final HttpResponse response = httpClient.execute(request);

         if (response == null) {

            throw new RuntimeException("No response from fulltext service");

         }

         final int statusCode = response.getStatusLine().getStatusCode();
         if (statusCode != 200) {

            throw new RuntimeException("Status code != 200 received from "
                  + "external fulltext service: " + statusCode);

         }

         final String jsonStr = EntityUtils.toString(response.getEntity(),
               "UTF-8");
         final JSONObject json = new JSONObject(jsonStr);

         return constructFulltextSearchList(json, query);

      } catch (IOException e) {

         throw new RuntimeException("Error submitting the keyword search"
               + " to the external service: " + e.getMessage());

      }

   }

   private FulltextSearchHit[] constructFulltextSearchList(
         JSONObject solrResultsJSON, FulltextSearchQuery query)
         throws JSONException {

      JSONObject resp = solrResultsJSON.getJSONObject("response");
      JSONArray docs = resp.getJSONArray("docs");

      List<FulltextSearchHit> searchHits = new ArrayList<FulltextSearchHit>(
            docs.length());
      for (int i = 0; i < docs.length(); i++) {

         JSONObject result = docs.getJSONObject(i);
         String sID = result.getString("id");

         String notes = " ";
         if (result.has("notes")) {
            JSONArray notesArray = result.getJSONArray("notes");

            if (notesArray.length() > 0) {
               StringBuilder notesBuilder = new StringBuilder(
                     notesArray.getString(0));
               for (int j = 1; j < notesArray.length(); j++) {
                  notesBuilder.append("; ");
                  notesBuilder.append(notesArray.getString(j));
               }
               notes = notesBuilder.toString();
            }
         }

         // TODO: include score
         FulltextSearchHit currentHit = new FulltextSearchHit(sID, 0.5, notes,
               query.getIncomingBindings(), query.getTargetType());

         searchHits.add(currentHit);
      }

      return searchHits.toArray(new FulltextSearchHit[searchHits.size()]);
   }

}
