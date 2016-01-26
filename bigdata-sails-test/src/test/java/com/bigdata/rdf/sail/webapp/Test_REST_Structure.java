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

package com.bigdata.rdf.sail.webapp;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import junit.framework.Test;

import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.server.Server;

import com.bigdata.BigdataStatics;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.webapp.client.ConnectOptions;
import com.bigdata.rdf.sail.webapp.client.HttpException;
import com.bigdata.rdf.sail.webapp.client.JettyResponseListener;
import com.bigdata.util.config.NicUtil;

/**
 * Proxied test suite for bootstrap and basic structure tests for the REST API.
 * 
 * @param <S>
 */
public class Test_REST_Structure<S extends IIndexManager> extends
		AbstractTestNanoSparqlClient<S> {

	public Test_REST_Structure() {

	}

	public Test_REST_Structure(final String name) {

		super(name);

	}

	public static Test suite() {

		return ProxySuiteHelper.suiteWhenStandalone(Test_REST_Structure.class,
                "test.*", TestMode.quads
//                , TestMode.sids
//                , TestMode.triples
                );
       
	}

   public void test_startup() throws Exception {

      assertTrue("open", m_fixture.isRunning());

   }
   
   public void testMultipleFixtures() throws Exception {
      
      assertTrue("open", m_fixture.isRunning());
      
      // create second fixture
      final String lnamespace = getName() + UUID.randomUUID();
      final Server altfixture = newFixture(lnamespace);
      try {
         final int altport = NanoSparqlServer.getLocalPort(altfixture);
         
           final String hostAddr = NicUtil.getIpAddress("default.nic", "default",
                   true/* loopbackOk */);

           if (hostAddr == null) {

               fail("Could not identify network address for this host.");

           }

           final String altrootURL = new URL("http", hostAddr, altport, ""/* contextPath */
           ).toExternalForm();

           final String altserviceURL = new URL("http", hostAddr, altport,
                   BigdataStatics.getContextPath()).toExternalForm();
           
           final String resp1 = doGET(m_serviceURL + "/status");
           final String resp2 = doGET(altserviceURL + "/status");
           
           // System.err.println("STANDARD:\n" + resp1);
           // System.err.println("ALT:\n" + resp2);

           // the status responses should not match since the ports of the servers are different
           assertFalse(resp1.equals(resp2));         
           
      } finally {
         altfixture.stop();
      }
      
   }

   /*
    * Verify the correct structure of the webapp.
    * 
    * TODO There should be tests here to verify that we do not allow listing of
    * the directory contents in the web application. This appears to be allowed
    * by default and we do not test to ensure that this option is disabled.
    * E.g.
    * 
    * http://172.16.0.185:8090/bigdata/html/
    * 
    * might list the directory contents.
    */

   /**
    * A marker placed into index.html so we can recognize when that page is
    * served.
    */
   private static final String JUNIT_TEST_MARKER_INDEX_HTML = "junit test marker: index.html";

   /**
    * bare URL of the server
    * 
    * <pre>
    * http://localhost:9999
    * </pre>
    * 
    * The response is should be <code>index.html</code> since we want the
    * bigdata webapp to respond for the top-level context.
    * 
    * <p>
    * Note: You must ensure that the client follows redirects using a standard
    * policy. This is necessary for tests of the webapp structure since the
    * container may respond with a redirect (302) to the location of the webapp
    * when the client requests the root URL.
    */
   public void test_webapp_structure_rootURL() throws Exception {

      final String content = doGET(m_rootURL);

      assertTrue(content.contains(JUNIT_TEST_MARKER_INDEX_HTML));

   }

   /**
    * URL with correct context path
    * 
    * <pre>
    * http://localhost:9999/bigdata
    * </pre>
    * 
    * The response is should be <code>index.html</code>, which is specified
    * through the welcome files list.
    */
   public void test_webapp_structure_contextPath() throws Exception {

      final String content = doGET(m_serviceURL);

      assertTrue(content.contains(JUNIT_TEST_MARKER_INDEX_HTML));
   }

   /**
    * URL with context path and index.html reference
    * 
    * <pre>
    * http://localhost:9999/bigdata/index.html
    * </pre>
    * 
    * This URL does NOT get mapped to anything (404).
    */
   public void test_webapp_structure_contextPath_indexHtml() throws Exception {

      try {

         doGET(m_serviceURL + "/index.html");

      } catch (HttpException ex) {

         assertEquals(404, ex.getStatusCode());

      }

   }

   /**
    * The <code>favicon.ico</code> file.
    * 
    * @see <a href="http://www.w3.org/2005/10/howto-favicon"> How to add a
    *      favicon </a>
    */
   public void test_webapp_structure_favicon() throws Exception {

      doGET(m_serviceURL + "/html/favicon.ico");

   }

   /**
    * The <code>/status</code> servlet responds.
    */
   public void test_webapp_structure_status() throws Exception {

      doGET(m_serviceURL + "/status");

   }

   /**
    * The <code>/counters</code> servlet responds.
    */
   public void test_webapp_structure_counters() throws Exception {

      doGET(m_serviceURL + "/counters");

   }

   // /**
   // * The <code>/namespace/</code> servlet responds (multi-tenancy API).
   // */
   // public void test_webapp_structure_namespace() throws Exception {
   //
   // doGET(m_serviceURL + "/namespace/");
   //
   // }

   /**
    * The fully qualified URL for <code>index.html</code>
    * 
    * <pre>
    * http://localhost:9999/bigdata/html/index.html
    * </pre>
    * 
    * The response is should be <code>index.html</code>, which is specified
    * through the welcome files list.
    */
   public void test_webapp_structure_contextPath_html_indexHtml()
         throws Exception {

      doGET(m_serviceURL + "/html/index.html");
   }

   private String doGET(final String url) throws Exception {

      JettyResponseListener response = null;

      final ConnectOptions opts = new ConnectOptions(url);
      opts.method = "GET";

      response = doConnect(opts);

      try {

         checkResponseCode(url, response);

         return response.getResponseBody();

      } finally {
         
         if (response != null)
            response.abort();
         
      }

   }

   /**
    * Throw an exception if the status code does not indicate success.
    * 
    * @param response
    *            The response.
    * 
    * @return The response.
    * 
    * @throws IOException
    */
   private static JettyResponseListener checkResponseCode(final String url,
         final JettyResponseListener response) throws IOException {

      final int rc = response.getStatus();

      if (rc < 200 || rc >= 300) {
         throw new HttpException(rc, "StatusCode=" + rc + ", StatusLine="
               + response.getReason() + ", headers="
               + response.getHeaders().toString()
               + ", ResponseBody="
               + response.getResponseBody());

      }

      if (log.isDebugEnabled()) {
         /*
          * write out the status list, headers, etc.
          */
         log.debug("*** Response ***");
         log.debug("Status Line: " + response.getReason());
      }

      return response;

   }

   /**
    * Connect to a SPARQL end point (GET or POST query only).
    * 
    * @param opts
    *            The connection options.
    * 
    * @return The connection.
    * 
    * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/619">
    *      RemoteRepository class should use application/x-www-form-urlencoded
    *      for large POST requests </a>
    */
   private JettyResponseListener doConnect(final ConnectOptions opts) throws Exception {

      /*
       * Generate the fully formed and encoded URL.
       */

      final StringBuilder urlString = new StringBuilder(opts.serviceURL);

      ConnectOptions.addQueryParams(urlString, opts.requestParams);

      final boolean isLongRequestURL = urlString.length() > 1024;

      if (isLongRequestURL && opts.method.equals("POST")
            && opts.entity == null) {

         /*
          * URL is too long. Reset the URL to just the service endpoint and
          * use application/x-www-form-urlencoded entity instead. Only in
          * cases where there is not already a request entity (SPARQL query
          * and SPARQL update).
          */

         urlString.setLength(0);
         urlString.append(opts.serviceURL);

         opts.entity = ConnectOptions.getFormEntity(opts.requestParams);

      } else if (isLongRequestURL && opts.method.equals("GET")
            && opts.entity == null) {

         /*
          * Convert automatically to a POST if the request URL is too long.
          * 
          * Note: [opts.entity == null] should always be true for a GET so
          * this bit is a paranoia check.
          */

         opts.method = "POST";

         urlString.setLength(0);
         urlString.append(opts.serviceURL);

         opts.entity = ConnectOptions.getFormEntity(opts.requestParams);

      }

      if (log.isDebugEnabled()) {
         log.debug("*** Request ***");
         log.debug(opts.serviceURL);
         log.debug(opts.method);
         log.debug("query=" + opts.getRequestParam("query"));
         log.debug(urlString.toString());
      }

      Request request = null;
      try {

         request = m_repo.getRemoteRepositoryManager().newRequest(urlString.toString(),
               opts.method);

         if (opts.requestHeaders != null) {

            for (Map.Entry<String, String> e : opts.requestHeaders
                  .entrySet()) {

               request.getHeaders().add(e.getKey(), e.getValue());

               if (log.isDebugEnabled())
                  log.debug(e.getKey() + ": " + e.getValue());

            }

         }

         if (opts.entity != null) {

            ((HttpEntityEnclosingRequestBase) request)
                  .setEntity(opts.entity);

         }

         final JettyResponseListener response = new JettyResponseListener(
               request, TimeUnit.SECONDS.toMillis(300));

         request.send(response);

         return response;

      } catch (Throwable t) {
         /*
          * If something goes wrong, then close the http connection.
          * Otherwise, the connection will be closed by the caller.
          */
         try {

            if (request != null)
               request.abort(t);

         } catch (Throwable t2) {
            // ignored.
         }
         throw new RuntimeException(opts.serviceURL + " : " + t, t);
      }

   }

}
