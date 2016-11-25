package com.acit.trendsdiscovery.util;
import java.util.logging.Logger;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.wink.json4j.JSONObject;

//import com.accenture.acit.retail.trends.dao.DataSourceDAO;

/****
 * Session Authentication for Graph DB
 * 
 */

public class SessionAuthenticationGraph {
	
	//private static final Logger log = Logger.getLogger(DataSourceDAO.class.getName());
	private static String authToken = null;

	/***
	 * Constructor
	 */
	public SessionAuthenticationGraph() {
		//TODO
	}
	
	/***
	 * Retrieve the current authentication token
	 * 
	 */
	public String getCurrentToken() {
		if(authToken == null) {
			authToken = generateAuthToken();
		}
		return authToken;
	}

	/***
	 * Generate new authentication token from Graph DB
	 */
	public String generateAuthToken() {
		
		System.out.println("Generate new token from Graph DB");
	
		//Graph Session URL
		String requestURL = System.getenv("GRAPH_DB_BASIC_URL");
		int urlLength = requestURL.length();
		requestURL = requestURL.substring(0, (urlLength-1));
		requestURL += "_session";
		
		//Graph credentials
		byte[] userpass = (System.getenv("GRAPH_DB_USER_ID") + ":" 
							+ System.getenv("GRAPH_DB_PASSWORD"))
							.getBytes();
		//Encoded credentials
		String encodedCred = Base64.encodeBase64String(userpass);
		
		try 
		{
			HttpClient client = HttpClients.createDefault();
			HttpGet httpGet = new HttpGet(requestURL);
			
			httpGet.setHeader("Authorization", "Basic "+encodedCred);
			HttpResponse httpResponse = client.execute(httpGet);
			
			HttpEntity httpEntity = httpResponse.getEntity();
			String content = EntityUtils.toString(httpEntity);
			
			JSONObject jsonContent = new JSONObject(content);
			authToken  = jsonContent.getString("gds-token");
			System.out.println("resultant token is : " + authToken);
			
			//Append the string 'gds-token' to the authentication token
			authToken = "gds-token " + authToken;

		}catch (Exception excep) {
			//log.log(Level.SEVERE, "Unable to fetch topics of a product from graphDB -> " + excep.getMessage(), excep);
			excep.printStackTrace();
		}
		return authToken;
	}
}
