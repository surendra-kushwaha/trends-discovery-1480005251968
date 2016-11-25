package com.acit.trendsdiscovery.util;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

/****
 * Graph DB REST utility
 * Execute the REST calls via POST and GET methods in this class
 * 
 */
public class GraphRESTUtil {

	//private static final Logger log = Logger.getLogger(GraphdbSearch.class.getName());

	private CloseableHttpClient client = null;
	private String gremlinURL = null;
	private String vertexURL = null;
	private String authToken = null;
	private SessionAuthenticationGraph sessionAuth = null;
	
	/***
	 * Constructor
	 */
	public GraphRESTUtil ()
	{
		initialize();
	}
	
	/***
	 * Initialize
	 */
	private void initialize()
	{
		client = HttpClients.createDefault();

		gremlinURL = System.getenv("GRAPH_DB_GREMLIN_URL");
		vertexURL = System.getenv("GRAPH_DB_BASIC_URL");
		//Get the current session token
		sessionAuth = new SessionAuthenticationGraph();
		authToken = sessionAuth.getCurrentToken();

	}
	
	/***
	 * Graph DB REST Invocation - HTTP GET
	 * @param queryString
	 * @return graphResponse
	 */
	public String invokeGraphGet(String queryString)
	{
		String graphResponse = null;
		try
		{
			HttpGet httpGet = new HttpGet(vertexURL + queryString);
			httpGet.setHeader("Authorization", authToken);
			
			// execute get request and retrieve the response
			HttpResponse httpResponse = client.execute(httpGet);
			StatusLine status = httpResponse.getStatusLine();

			//log.info("After setting the HTTP Get header : " + status.getStatusCode());

			//Generate a new token if the query results in an unauthorized exception
			if(status.getStatusCode() == HttpStatus.SC_UNAUTHORIZED)
			{
				authToken = sessionAuth.generateAuthToken();
				httpGet = new HttpGet(vertexURL + queryString);
				httpGet.setHeader("Authorization", authToken);
				httpResponse = client.execute(httpGet);
			}
			
			HttpEntity httpEntity = httpResponse.getEntity();
			graphResponse = EntityUtils.toString(httpEntity);
			EntityUtils.consume(httpEntity);

		} catch (Exception excep) {
			//log.log(Level.SEVERE, "Exception with Graph GET call -> " + excep.getMessage(), excep);
		}
		
		return graphResponse;
	}

	/****
	 * Graph DB REST Invocation - HTTP POST
	 * @param stringEntity
	 * @return graphResponse
	 */
	public String invokeGraphPost(StringEntity stringEntity)
	{
		String graphResponse = null;
		try
		{
			HttpPost httpPost = new HttpPost(gremlinURL);
			httpPost.setEntity(stringEntity);
			httpPost.setHeader("Authorization", authToken);
			
			// send the post request and retrieve the response
			HttpResponse httpResponse = client.execute(httpPost);
			StatusLine status = httpResponse.getStatusLine();

			//log.info("After setting the HTTP Post header : " + status.getStatusCode());

			//Generate a new token if the query results in an unauthorized exception
			if(status.getStatusCode() == HttpStatus.SC_UNAUTHORIZED)
			{
				authToken = sessionAuth.generateAuthToken();
				httpPost = new HttpPost(gremlinURL);
				httpPost.setEntity(stringEntity);
				httpPost.setHeader("Authorization", authToken);
				httpResponse = client.execute(httpPost);
			}
			
			HttpEntity httpEntity = httpResponse.getEntity();
			graphResponse = EntityUtils.toString(httpEntity);
			EntityUtils.consume(httpEntity);
			
		} catch (Exception excep) {
			//log.log(Level.SEVERE, "Exception with Graph POST call -> " + excep.getMessage(), excep);
		}

		return graphResponse;
	}
}
