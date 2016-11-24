package example.nosql;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.annotation.MultipartConfig;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

@WebServlet("/getTrends")
@MultipartConfig()
public class AttachServlet extends HttpServlet {

	private static final int readBufferSize = 8192;
	private static final long serialVersionUID = 1L;

	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		
		doGet(request,response);
	}

	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		
		response.getWriter().print(trendsFinder());
		
		
	}
	
	public String trendsFinder(){
		JsonObject responseJson=null;
		JsonElement je=null;
		String trends=null;
		try{			
			String url = "http://api.walmartlabs.com/v1/search?apiKey=agevmwa5rhme979szegdj3v6&query=PHOTO%20SHADOW%20BOX%20TRAY&sort=customerRating&order=desc&numItems=5";
			HttpClient client = HttpClientBuilder.create().build();
			HttpGet request = new HttpGet(url);
			request.addHeader("User-Agent", "");
			HttpResponse response = client.execute(request);
			
			String result  = EntityUtils.toString(response.getEntity());
			//JsonObject responseJson = (JsonObject)result;
			JsonParser parser = new JsonParser();
			responseJson = parser.parse(result).getAsJsonObject();
			
			je=responseJson.getAsJsonArray("items").get(0);
			
			JsonParser parser1 = new JsonParser();
			responseJson = parser1.parse(je.toString()).getAsJsonObject();
			 trends=responseJson.get("categoryPath").getAsString();
			
			
			
			System.out.println("Response status"+response.getStatusLine().getStatusCode());
			System.out.println("Response from wallmart api:##"+response.toString());
		}catch(Exception e){System.out.println(e);
			//logger.error(e);
		}
		return je.toString()+"::"+trends;
	}

}
