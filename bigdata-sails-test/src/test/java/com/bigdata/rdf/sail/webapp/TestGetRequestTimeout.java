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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.Principal;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Locale;
import java.util.Map;

import javax.servlet.AsyncContext;
import javax.servlet.DispatcherType;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpUpgradeHandler;
import javax.servlet.http.Part;

import junit.framework.TestCase2;


/**
 * Unit tests for the {@link BigdataRDFContext#getQueryTimeout} method.
 */
public class TestGetRequestTimeout extends TestCase2 {
	
	public void testGetRequestTimeout() throws Exception {
		
		final String httpHeaderBigdataMaxQueryMillis = "1000";
		final String maxQuryTimeMillis = "1000";
		final String timeout = "1"; 
		long configTimeout = 1;
		
		
		final HttpServletRequest req = new MockRequest(//
				httpHeaderBigdataMaxQueryMillis, maxQuryTimeMillis, timeout);
        
        long actual = BigdataRDFContext.getQueryTimeout(req, configTimeout);
        
        long expected = configTimeout;
       	
		assertEquals(expected , actual);

	}
	
	public void testGetRequestTimeout1() throws Exception {
		
		final String httpHeaderBigdataMaxQueryMillis = "1";
		final String maxQuryTimeMillis = "1000";
		final String timeout = "1"; 
		long configTimeout = 1000;
		
		final HttpServletRequest req = new MockRequest(//
				httpHeaderBigdataMaxQueryMillis, maxQuryTimeMillis, timeout);
        
        final long actual = BigdataRDFContext.getQueryTimeout(req, configTimeout);
        
        final long expected = Long.parseLong(httpHeaderBigdataMaxQueryMillis);
       	
		assertEquals(expected , actual);
       

	}
	
	public void testGetRequestTimeout2() throws Exception {
		 
		final String httpHeaderBigdataMaxQueryMillis = "1000";
		final String maxQuryTimeMillis = "1";
		final String timeout = "1"; 
		long configTimeout = 1000;
		
		
		final HttpServletRequest req = new MockRequest(//
				httpHeaderBigdataMaxQueryMillis, maxQuryTimeMillis, timeout);
        
        final long actual = BigdataRDFContext.getQueryTimeout(req, configTimeout);
        
        final long expected = Long.parseLong(maxQuryTimeMillis);
       	
		assertEquals(expected, actual);
        

	}
	
	public void testGetRequestTimeout3() throws Exception {
		
		final String httpHeaderBigdataMaxQueryMillis = "10000";
		final String maxQuryTimeMillis = "10000";
		final String timeout = "1"; 
		long configTimeout = 10000;
		
		
		final HttpServletRequest req = new MockRequest(//
				httpHeaderBigdataMaxQueryMillis, maxQuryTimeMillis, timeout);
        
        final long actual = BigdataRDFContext.getQueryTimeout(req, configTimeout);
        
        final long expected = Long.parseLong(timeout)*1000;
       	
		assertEquals(expected , actual);
 
	}
	
	/**
	 * A mock class used locally for testing purposes.
	 */
	
	private class MockRequest implements HttpServletRequest {
		
		private String maxQuryTimeMillis;
		private String timeout;
		private String httpHeaderBigdataMaxQueryMillis;

		private MockRequest(String httpHeaderBigdataMaxQueryMillis, String maxQuryTimeMillis, String timeout) {
			
			this.httpHeaderBigdataMaxQueryMillis = httpHeaderBigdataMaxQueryMillis;
			this.maxQuryTimeMillis = maxQuryTimeMillis;
			this.timeout = timeout;
			
		}

		public String getParameter(String name) {
			if (name.equals(BigdataRDFContext.TIMEOUT))
				return timeout;
			else if (name.equals(BigdataRDFContext.MAX_QUERY_TIME_MILLIS)) 
				return maxQuryTimeMillis;
			return null;
		}

		@Override
		public String getHeader(String name) {
			if (name.equals(BigdataRDFContext.HTTP_HEADER_BIGDATA_MAX_QUERY_MILLIS))
				return httpHeaderBigdataMaxQueryMillis;
			else 
				return null;
		}

		@Override
		public Object getAttribute(String name) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Enumeration<String> getAttributeNames() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getCharacterEncoding() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void setCharacterEncoding(String env)
				throws UnsupportedEncodingException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public int getContentLength() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public long getContentLengthLong() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public String getContentType() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public ServletInputStream getInputStream() throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Enumeration<String> getParameterNames() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String[] getParameterValues(String name) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Map<String, String[]> getParameterMap() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getProtocol() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getScheme() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getServerName() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public int getServerPort() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public BufferedReader getReader() throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getRemoteAddr() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getRemoteHost() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void setAttribute(String name, Object o) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void removeAttribute(String name) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public Locale getLocale() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Enumeration<Locale> getLocales() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public boolean isSecure() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public RequestDispatcher getRequestDispatcher(String path) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getRealPath(String path) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public int getRemotePort() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public String getLocalName() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getLocalAddr() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public int getLocalPort() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public ServletContext getServletContext() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public AsyncContext startAsync() throws IllegalStateException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public AsyncContext startAsync(ServletRequest servletRequest,
				ServletResponse servletResponse) throws IllegalStateException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public boolean isAsyncStarted() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean isAsyncSupported() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public AsyncContext getAsyncContext() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public DispatcherType getDispatcherType() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getAuthType() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Cookie[] getCookies() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public long getDateHeader(String name) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public Enumeration<String> getHeaders(String name) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Enumeration<String> getHeaderNames() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public int getIntHeader(String name) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public String getMethod() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getPathInfo() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getPathTranslated() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getContextPath() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getQueryString() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getRemoteUser() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public boolean isUserInRole(String role) {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public Principal getUserPrincipal() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getRequestedSessionId() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getRequestURI() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public StringBuffer getRequestURL() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getServletPath() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public HttpSession getSession(boolean create) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public HttpSession getSession() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String changeSessionId() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public boolean isRequestedSessionIdValid() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean isRequestedSessionIdFromCookie() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean isRequestedSessionIdFromURL() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean isRequestedSessionIdFromUrl() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean authenticate(HttpServletResponse response)
				throws IOException, ServletException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public void login(String username, String password)
				throws ServletException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void logout() throws ServletException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public Collection<Part> getParts() throws IOException, ServletException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Part getPart(String name) throws IOException, ServletException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public <T extends HttpUpgradeHandler> T upgrade(Class<T> handlerClass)
				throws IOException, ServletException {
			// TODO Auto-generated method stub
			return null;
		}
	}
    
}
