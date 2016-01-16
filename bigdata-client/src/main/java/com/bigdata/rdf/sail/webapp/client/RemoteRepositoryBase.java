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
package com.bigdata.rdf.sail.webapp.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.log4j.Logger;
import org.openrdf.model.Graph;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.LinkedHashModel;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RDFWriterFactory;
import org.openrdf.rio.RDFWriterRegistry;
import org.xml.sax.Attributes;
import org.xml.sax.ext.DefaultHandler2;

/**
 * Base class providing some common functionality.
 * 
 * @author bryan
 * 
 */
public class RemoteRepositoryBase extends RemoteRepositoryDecls {

   private static final transient Logger log = Logger
         .getLogger(RemoteRepositoryDecls.class);

   /**
    * Return the web application context path for the default deployment of the
    * bigdata web application.
    * 
    * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/730" >
    *      Allow configuration of embedded NSS jetty server using jetty-web.xml
    *      </a>
    * 
    *      FIXME Configure ContextPath, but NOT with BigdataStatics since that
    *      will drag in code outside of this package. Instead, make this a
    *      System property or constructor property or parse it out of the
    *      request URL.
    * 
    *      FIXME Actually, the situation is a bit worse. The ContextPath might
    *      not appear in the public version of the URL. If it does not, then we
    *      won't be able to use the load balancer....
    */
   protected static final String getContextPath() {

       return "/bigdata";
       
   }

   /**
    * Throw an exception if the status code does not indicate success.
    * 
    * @param inputStreamResponseListener
    *            The response.
    *            
    * @return The response.
    * 
    * @throws IOException
    */
   static public JettyResponseListener checkResponseCode(final JettyResponseListener responseListener)
           throws IOException {
       
       final int rc = responseListener.getStatus();
       
       if (rc < 200 || rc >= 300) {
        
           throw new HttpException(rc, "Status Code=" + rc + ", Status Line="
                   + responseListener.getReason() + ", Response="
                   + responseListener.getResponseBody());

       }

       if (log.isDebugEnabled()) {
           /*
            * write out the status list, headers, etc.
            */
           log.debug("*** Response ***");
           log.debug("Status Line: " + responseListener.getReason());
       }

       return responseListener;
       
   }

   /**
    * Utility method to turn a {@link GraphQueryResult} into a {@link Graph}.
    * 
    * @param result
    *            The {@link GraphQueryResult}.
    * 
    * @return The {@link Graph}.
    * 
    * @throws Exception
    */
   static public Graph asGraph(final GraphQueryResult result) throws Exception {

       final Graph g = new LinkedHashModel();

       while (result.hasNext()) {

           g.add(result.next());

       }

       return g;

   }

   /**
    * Serialize an iteration of statements into a byte[] to send across the
    * wire.
    */
   protected static byte[] serialize(final Iterable<? extends Statement> stmts,
           final RDFFormat format) throws Exception {
       
       final RDFWriterFactory writerFactory = 
           RDFWriterRegistry.getInstance().get(format);

       final ByteArrayOutputStream baos = new ByteArrayOutputStream();
       
       final RDFWriter writer = writerFactory.getWriter(baos);
       
       writer.startRDF();
       
       for (Statement stmt : stmts) {
       
           writer.handleStatement(stmt);
           
       }

       writer.endRDF();
       
       final byte[] data = baos.toByteArray();
       
       return data;

   }

   static protected MutationResult mutationResults(final JettyResponseListener response)
         throws Exception {

     try {

         final String contentType = response.getContentType();

         if (!contentType.startsWith(IMimeTypes.MIME_APPLICATION_XML)) {

             throw new RuntimeException("Expecting Content-Type of "
                     + IMimeTypes.MIME_APPLICATION_XML + ", not "
                     + contentType);

         }

         final SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
         
         final AtomicLong mutationCount = new AtomicLong();
         final AtomicLong elapsedMillis = new AtomicLong();
         
         /*
          * For example: <data modified="5" milliseconds="112"/>
          */
         parser.parse(response.getInputStream(), new DefaultHandler2(){

            @Override
             public void startElement(final String uri,
                     final String localName, final String qName,
                     final Attributes attributes) {

                 if (!"data".equals(qName))
                     throw new RuntimeException("Expecting: 'data', but have: uri=" + uri
                             + ", localName=" + localName + ", qName="
                             + qName);

                 mutationCount.set(Long.valueOf(attributes
                         .getValue("modified")));

                 elapsedMillis.set(Long.valueOf(attributes
                         .getValue("milliseconds")));
                        
             }
             
         });
         
         // done.
         return new MutationResult(mutationCount.get(), elapsedMillis.get());

     } finally {

      if (response != null) {
         response.abort();
      }
      
     }

 }

   /**
    * Accept and parse a boolean response (NSS specific response type).
    */
   static protected BooleanResult booleanResults(
         final JettyResponseListener response) throws Exception {

     try {
         
         final String contentType = response.getContentType();

         if (!contentType.startsWith(IMimeTypes.MIME_APPLICATION_XML)) {

             throw new RuntimeException("Expecting Content-Type of "
                     + IMimeTypes.MIME_APPLICATION_XML + ", not "
                     + contentType);

         }

         final SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
         
         final AtomicBoolean result = new AtomicBoolean();
         final AtomicLong elapsedMillis = new AtomicLong();

         /*
          * For example: <data rangeCount="5" milliseconds="112"/>
          */
         parser.parse(response.getInputStream(), new DefaultHandler2(){

            @Override
             public void startElement(final String uri,
                     final String localName, final String qName,
                     final Attributes attributes) {

                 if (!"data".equals(qName))
                     throw new RuntimeException("Expecting: 'data', but have: uri=" + uri
                             + ", localName=" + localName + ", qName="
                             + qName);

                 result.set(Boolean.valueOf(attributes
                         .getValue("result")));

                 elapsedMillis.set(Long.valueOf(attributes
                         .getValue("milliseconds")));
                        
             }
             
         });
         
         // done.
         return new BooleanResult(result.get(), elapsedMillis.get());

     } finally {

      if (response != null) {
         response.abort();
      }
      
     }

 }

   /**
    * Accept and parse a range count response (NSS specific response type).
    */
   static protected RangeCountResult rangeCountResults(
           final JettyResponseListener response) throws Exception {

       try {
           
           final String contentType = response.getContentType();

           if (!contentType.startsWith(IMimeTypes.MIME_APPLICATION_XML)) {

               throw new RuntimeException("Expecting Content-Type of "
                       + IMimeTypes.MIME_APPLICATION_XML + ", not "
                       + contentType);

           }

           final SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
           
           final AtomicLong rangeCount = new AtomicLong();
           final AtomicLong elapsedMillis = new AtomicLong();

           /*
            * For example: <data rangeCount="5" milliseconds="112"/>
            */
           parser.parse(response.getInputStream(), new DefaultHandler2(){

              @Override
               public void startElement(final String uri,
                       final String localName, final String qName,
                       final Attributes attributes) {

                   if (!"data".equals(qName))
                       throw new RuntimeException("Expecting: 'data', but have: uri=" + uri
                               + ", localName=" + localName + ", qName="
                               + qName);

                   rangeCount.set(Long.valueOf(attributes
                           .getValue("rangeCount")));

                   elapsedMillis.set(Long.valueOf(attributes
                           .getValue("milliseconds")));
                          
               }
               
           });
           
           // done.
           return new RangeCountResult(rangeCount.get(), elapsedMillis.get());

       } finally {

        if (response != null) {
           response.abort();
        }
        
       }

   }

   static protected ContextsResult contextsResults(
           final JettyResponseListener response) throws Exception {

       try {
           
           final String contentType = response.getContentType();

           if (!contentType.startsWith(IMimeTypes.MIME_APPLICATION_XML)) {

               throw new RuntimeException("Expecting Content-Type of "
                       + IMimeTypes.MIME_APPLICATION_XML + ", not "
                       + contentType);

           }

           final SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
           
           final Collection<Resource> contexts = 
                 Collections.synchronizedCollection(new LinkedList<Resource>());

           /*
            * For example: 
            * <contexts>
            * <context uri="http://foo"/>
            * <context uri="http://bar"/>
            * </contexts>
            */
           parser.parse(response.getInputStream(), new DefaultHandler2(){

              @Override
               public void startElement(final String uri,
                       final String localName, final String qName,
                       final Attributes attributes) {

                   if ("context".equals(qName))
                    contexts.add(new URIImpl(attributes.getValue("uri")));

               }
               
           });
           
           // done.
           return new ContextsResult(contexts);

       } finally {

        if (response != null) {
           response.abort();
        }
        
       }

   }

   /**
    * Convert an array of URIs to an array of URI strings.
    */
   static protected String[] toStrings(final Resource[] resources) {
     
     if (resources == null)
        return null;
     
     if (resources.length == 0)
        return new String[0];

     final String[] uris = new String[resources.length];
     
     for (int i = 0; i < resources.length; i++) {
        
        uris[i] = resources[i].stringValue();
        
     }
     
     return uris;
     
   }

}
