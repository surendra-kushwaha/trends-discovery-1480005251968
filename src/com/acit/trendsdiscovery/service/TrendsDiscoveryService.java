package com.acit.trendsdiscovery.service;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

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
import org.apache.wink.json4j.JSONObject;

import com.acit.trendsdiscovery.dao.MetaKeywordsDAO;
import com.acit.trendsdiscovery.model.MetaKeywords;
import com.acit.trendsdiscovery.util.SemrushClient;
import com.acit.trendsdiscovery.util.TopicAssociationGraphdb;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

@WebServlet("/findTrends")
@MultipartConfig()
public class TrendsDiscoveryService extends HttpServlet {

	private static final int readBufferSize = 8192;
	private static final long serialVersionUID = 1L;
	MetaKeywordsDAO metaKeywordsDAO = new MetaKeywordsDAO();
	TopicAssociationGraphdb topicAssocGraphdb = new TopicAssociationGraphdb();
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		
		doGet(request,response);
	}

	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		
		
		/*JSONObject categories = new JSONObject();
		System.out.println("Before category call ");
    	categories = topicAssocGraphdb.getCategories();
    	System.out.println("after category call ");*/
        //System.out.println("Categories in memcache not available  : "+categories);
		trendsFinderSemrush();
		//response.getWriter().print(trendsFinder()+"Categories ::"+categories);
        //response.getWriter().print(trendsFinder());
		response.getWriter().print("Trends data uploaded to master data.");
		
		
	}
	
	public void trendsFinderSemrush(){
		JsonObject responseJson=null;
		JsonElement je=null;
		String trends=null;
		String trendsRetrived[]=new String[5];
		try{
			
			List<String> departments=metaKeywordsDAO.getDepartments();
			
			System.out.println("departments iterator::"+departments.size());
			Iterator<String> iterator = departments.iterator();
			while (iterator.hasNext()) {
				//System.out.println(iterator.next());
				String mikDepartment=iterator.next();
				//System.out.println("mikSubclass value::"+mikSubclass);
				mikDepartment = URLEncoder.encode(mikDepartment, "UTF-8");
				System.out.println("mikDepartment encoded value::"+mikDepartment);
						
					//String url = "http://api.walmartlabs.com/v1/search?apiKey=agevmwa5rhme979szegdj3v6&query=PHOTO%20SHADOW%20BOX%20TRAY&sort=customerRating&order=desc&numItems=5";
					String url = "http://api.walmartlabs.com/v1/search?apiKey=agevmwa5rhme979szegdj3v6&query="+mikDepartment+"&sort=relevance&numItems=5";
					HttpClient client = HttpClientBuilder.create().build();
					HttpGet request = new HttpGet(url);
					request.addHeader("User-Agent", "");
					HttpResponse response = client.execute(request);
					
					String result  = EntityUtils.toString(response.getEntity());
					//JsonObject responseJson = (JsonObject)result;
					JsonParser parser = new JsonParser();
					responseJson = parser.parse(result).getAsJsonObject();
					JsonArray jsonArray=responseJson.getAsJsonArray("items");
					
					for(int i=0;i<jsonArray.size();i++){				
						je=jsonArray.get(i);			
						JsonParser parser1 = new JsonParser();
						responseJson = parser1.parse(je.toString()).getAsJsonObject();
						trends=responseJson.get("productUrl").getAsString();
						
						//String organicUrl=trends.substring(trends.lastIndexOf("?l=")+3,trends.lastIndexOf("47150667")-1);
						
						//trends=trends.substring(trends.lastIndexOf("/")+1);
						
						//System.out.println("trends count:"+i+"::"+trends);						
						//trendsRetrived[i]=trends;
						
						//Semrush call
						System.out.println("getTrendsForAllKeywords Rest ");
						SemrushClient semrushClient = new SemrushClient();						
						//semrushClient.getTrendsFromSemrushAPI("",System.getenv("exportColumns"));						
						//InputStreamReader in = semrushClient.getTrendsFromSemrushAPI("JEWELRY","Ph");
						InputStreamReader in = semrushClient.getTrendsFromSemrushAPI("http://www.walmart.com/","Ph");
						//InputStreamReader in = semrushClient.getTrendsFromSemrushAPI(organicUrl,System.getenv("exportColumns"));
						List<String> keywordResult=null;
						if(in != null){							
							keywordResult = semrushClient.processKeywordResults("http://www.walmart.com/", "Ph",in);
							//keywordResult = semrushClient.processKeywordResults(organicUrl, "Ph",in);
							//JSONObject keywordResult = semrushClient.processKeywordResults("Kids crafts", System.getenv("exportColumns"),in);	
							System.out.println("Keyword list : "+keywordResult);							
							// Send the Semrush Keyword Result to Message Hub
							//String topicName = System.getenv("topicName");
							//postClient.postSemurshToMessageHub("topic=" + topicName + "&data=" + keywordResult);

							//System.out.println("Keyword : "+ metaKeyword);
							
						}
						
						Iterator<String> keyIterator = keywordResult.iterator();
						while (keyIterator.hasNext()) {
							MetaKeywords metaKeyWords=new MetaKeywords();
							//metaKeyWords.setKeywordName(trends);
							String keyword=keyIterator.next();
							System.out.println("keyword##"+keyword);
							metaKeyWords.setKeywordName(keyword);
							//metaKeyWords.setKeywordName("fall");
							metaKeyWords.setActive("Y");
							metaKeyWords.setLastUpdateDttm(new Date());
							metaKeyWords.setModifiedBy("Raghav");
							//Add to master table
							boolean semrushUpdate=metaKeywordsDAO.addSemurshMetaKeyword(metaKeyWords);
							boolean twitterUpdate=metaKeywordsDAO.addTwitterMetaKeyword(metaKeyWords);
							
							System.out.println("update status twitter:"+twitterUpdate);
							System.out.println("update status semrush:"+semrushUpdate);
						}
						
					}
					
					break;
			
			}
			
			
			//System.out.println("Response status"+response.getStatusLine().getStatusCode());
			//System.out.println("Response from wallmart api:##"+response.toString());
		}catch(Exception e){System.out.println(e);
			//logger.error(e);
		}
		//return trendsRetrived[0]+":"+trendsRetrived[1]+"::"+trends;
	}
	
	public void trendsFinder(){
		JsonObject responseJson=null;
		JsonElement je=null;
		String trends=null;
		String trendsRetrived[]=new String[5];
		try{
			
			List<String> trendsData=metaKeywordsDAO.getTrendsDiscoveryData();
			
			System.out.println("trendsData iterator::"+trendsData.size());
			Iterator<String> iterator = trendsData.iterator();
			while (iterator.hasNext()) {
					//System.out.println(iterator.next());
					String mikSubclass=iterator.next();
						
					//String url = "http://api.walmartlabs.com/v1/search?apiKey=agevmwa5rhme979szegdj3v6&query=PHOTO%20SHADOW%20BOX%20TRAY&sort=customerRating&order=desc&numItems=5";
					String url = "http://api.walmartlabs.com/v1/search?apiKey=agevmwa5rhme979szegdj3v6&query="+mikSubclass+"&sort=relevance&numItems=5";
					HttpClient client = HttpClientBuilder.create().build();
					HttpGet request = new HttpGet(url);
					request.addHeader("User-Agent", "");
					HttpResponse response = client.execute(request);
					
					String result  = EntityUtils.toString(response.getEntity());
					//JsonObject responseJson = (JsonObject)result;
					JsonParser parser = new JsonParser();
					responseJson = parser.parse(result).getAsJsonObject();
					JsonArray jsonArray=responseJson.getAsJsonArray("items");
					
					for(int i=0;i<jsonArray.size();i++){				
						je=jsonArray.get(i);			
						JsonParser parser1 = new JsonParser();
						responseJson = parser1.parse(je.toString()).getAsJsonObject();
						trends=responseJson.get("categoryPath").getAsString();
						trends=trends.substring(trends.lastIndexOf("/")+1);
						
						System.out.println("trends count:"+i+"::"+trends);
						
						trendsRetrived[i]=trends;
						
						MetaKeywords metaKeyWords=new MetaKeywords();
						//metaKeyWords.setKeywordName(trends);
						metaKeyWords.setKeywordName("fall");
						metaKeyWords.setActive("Y");
						metaKeyWords.setLastUpdateDttm(new Date());
						metaKeyWords.setModifiedBy("Raghav");
						//Add to master table
						boolean semrushUpdate=metaKeywordsDAO.addSemurshMetaKeyword(metaKeyWords);
						boolean twitterUpdate=metaKeywordsDAO.addTwitterMetaKeyword(metaKeyWords);
						
						System.out.println("update status twitter:"+twitterUpdate);
						System.out.println("update status semrush:"+semrushUpdate);
						
					}
			
			}
			
			
			//System.out.println("Response status"+response.getStatusLine().getStatusCode());
			//System.out.println("Response from wallmart api:##"+response.toString());
		}catch(Exception e){System.out.println(e);
			//logger.error(e);
		}
		//return trendsRetrived[0]+":"+trendsRetrived[1]+"::"+trends;
	}

}
