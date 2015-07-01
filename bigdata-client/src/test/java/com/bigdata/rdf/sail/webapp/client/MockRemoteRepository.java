package com.bigdata.rdf.sail.webapp.client;

import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpRequest;
import org.eclipse.jetty.client.HttpResponse;
import org.eclipse.jetty.client.api.Response.ResponseListener;
import org.eclipse.jetty.client.api.Result;
import org.eclipse.jetty.http.HttpHeader;

public class MockRemoteRepository extends RemoteRepository {

	public final static class Data {
		public ConnectOptions opts;
		public HttpRequest request;
		public IPreparedTupleQuery query;
		protected List<ResponseListener> listeners;
	}
	public Data data;

	public MockRemoteRepository(RemoteRepositoryManager mgr, String sparqlEndpointURL, IRemoteTx tx, Data data) {
		super(mgr, sparqlEndpointURL, tx);
		this.data = data;
	}

	public static MockRemoteRepository create(final String responseJson) {
		// pojo to retrieve values from mock service
		final Data data = new Data();

		String serviceURL = "localhost";
		HttpClient httpClient = new HttpClient() {
			@Override
			protected void send(HttpRequest request,
					List<ResponseListener> listeners) {
				// Store HTTP request
				data.request = request;
				data.listeners = listeners;
				for (ResponseListener listener: listeners) {
					if (listener instanceof JettyResponseListener) {
						HttpResponse response = new HttpResponse(request, null){
							@Override
							public int getStatus() {
								return 200;
							};
							
//							@Override
//							public InputStream getInputStream() {
//								return new ByteArrayInputStream(responseJson.getBytes(Charset.forName("UTF-8")));
//							}
						};
						response.getHeaders().add(HttpHeader.CONTENT_TYPE, "application/json");
						((JettyResponseListener)listener).onHeaders(response);
						java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(responseJson.length());
						buf.put(responseJson.getBytes(Charset.forName("UTF-8")));
						buf.flip();
						((JettyResponseListener)listener).onContent(response, buf);
						((JettyResponseListener)listener).onSuccess(response);
						((JettyResponseListener)listener).onComplete(new Result(request, response));
					}
				}
			}
			@Override
			public boolean isStopped() {
				return false;
			}
		};
		Executor executor = Executors.newCachedThreadPool();
		RemoteRepositoryManager mgr = new RemoteRepositoryManager(serviceURL,
	            httpClient, executor) {
			@Override
			public JettyResponseListener doConnect(ConnectOptions opts) throws Exception {
				// Store connection options
				data.opts = opts;
				return super.doConnect(opts);
//				HttpRequest request = new HttpRequest(new HttpClient(), null, URI.create("")){};
//				return new JettyResponseListener(request, 0){
//					@Override
//					public Response get(long timeout, TimeUnit unit)
//							throws InterruptedException, TimeoutException,
//							ExecutionException {
//						return new HttpResponse(request, null);
//					}
//					@Override
//					public int getStatus() throws IOException {
//						return 200;
//					}
//					@Override
//					public String getContentType() throws IOException {
//						return "application/json";
//					}
//					@Override
//					public InputStream getInputStream() {
//						return new ByteArrayInputStream(responseJson.getBytes(Charset.forName("UTF-8")));
//					}
//				};
			}
		};

		return new MockRemoteRepository(mgr, serviceURL, null, data);
	}

	@Override
	public IPreparedTupleQuery prepareTupleQuery(String query)
			throws Exception {
		// Store IPreparedTupleQuery
		return data.query = super.prepareTupleQuery(query);
	}
	
}
