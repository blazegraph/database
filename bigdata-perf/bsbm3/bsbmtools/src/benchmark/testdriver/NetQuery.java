package benchmark.testdriver;

import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.*;
import java.io.*;

public class NetQuery {
	HttpURLConnection conn;
	Long start;
	Long end;
	
	protected NetQuery(String serviceURL, String query, byte queryType, String defaultGraph, int timeout) {
		try {
			String urlString = null;
			if(queryType==Query.UPDATE_TYPE)
				urlString = serviceURL;
			else
				urlString = serviceURL + "?query=" + URLEncoder.encode(query, "UTF-8");
//			System.out.println(urlString);
			
			if(defaultGraph!=null)
				urlString +=  "&default-graph-uri=" + defaultGraph;

			URL url = new URL(urlString);
			conn = (HttpURLConnection)url.openConnection();

			configureConnection(query, queryType, timeout);
		} catch(UnsupportedEncodingException e) {
			System.err.println(e.toString());
			System.exit(-1);
		} catch(MalformedURLException e) {
			System.err.println(e.toString());
			System.exit(-1);
		} catch(IOException e) {
			System.err.println(e.toString());
			System.exit(-1);
		}
	}

	private void configureConnection(String query, byte queryType, int timeout)
			throws ProtocolException, IOException{
		if(queryType==Query.UPDATE_TYPE)
			conn.setRequestMethod("POST");
		else
			conn.setRequestMethod("GET");
		conn.setDefaultUseCaches(false);
		conn.setDoOutput(true);
		conn.setUseCaches(false);
		conn.setReadTimeout(timeout);
		if(queryType==Query.DESCRIBE_TYPE || queryType==Query.CONSTRUCT_TYPE)
			conn.setRequestProperty("Accept", "application/rdf+xml");
		else
			conn.setRequestProperty("Accept", "application/sparql-results+xml");
		
		if(queryType==Query.UPDATE_TYPE) {
			conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
			OutputStream out = conn.getOutputStream();
			out.write("request=".getBytes());
			out.write(URLEncoder.encode(query, "UTF-8").getBytes());
			out.flush();
		}
	}
	
	protected InputStream exec() {
		try {
			conn.connect();

		} catch(IOException e) {
			System.err.println("Could not connect to SPARQL Service.");
			e.printStackTrace();
			System.exit(-1);
		}
		try {
			start = System.nanoTime();
			int rc = conn.getResponseCode();
			if(rc < 200 || rc >= 300) {
				System.err.println("Query execution: Received error code " + rc + " from server for Query:\n");
				System.err.println(URLDecoder.decode(conn.getURL().getQuery(),"UTF-8"));
			}
			return conn.getInputStream();
		} catch(SocketTimeoutException e) {
			return null;
		} catch(IOException e) {
			System.err.println("Query execution error:");
			e.printStackTrace();
			System.exit(-1);
			return null;
		}

	}
	
	protected double getExecutionTimeInSeconds() {
		end = System.nanoTime();
		Long interval = end-start;
		Thread.yield();
		return interval.doubleValue()/1000000000;
	}
	
	protected void close() {
		conn.disconnect();
		conn = null;
	}
}
