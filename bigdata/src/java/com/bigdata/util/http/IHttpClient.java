package com.bigdata.util.http;

public interface IHttpClient {
	
	public IHttpResponse GET(final String url) throws Exception;
	
	public IHttpEntity createEntity();
	
	public IHttpRequest newRequest(String uri, String method);
}
