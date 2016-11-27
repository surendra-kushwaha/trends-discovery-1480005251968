package com.acit.trendsdiscovery.scheduler;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.TimerTask;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
//import org.apache.log4j.Logger;
import org.apache.http.util.EntityUtils;

import com.acit.trendsdiscovery.dao.MetaKeywordsDAO;
import com.acit.trendsdiscovery.model.MetaKeywords;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

 
/**
 * Class to extend and implement custom "Cron Job" equivalent because with
 * bluemix apps, service deployer does have access to VM's cron capability
 * 
 * @author surendra
 * 
 */
public class TaskScheduler extends TimerTask {
 
//private static final Log LOG = LogFactory.getLog(TaskScheduler.class);
	public String cron_name;
	public String command;
	public int interval;
	public boolean isEnabled;
	static String fedAUth = null;
	  //private final Logger logger = Logger.getLogger(TaskScheduler.class);

@Override
public void run() 
{    
    try{
    	if (this.isEnabled) {
    		System.out.println("Trends Discovery Scheduler");
    		consumerScheduler();
    	} else {
    		this.cancel();
    	}
	}catch(Exception e){
		//LOG.info("Exception occured" + e);
	}
    
}
  
public void setCronName(String value) {
        cron_name = value;
}
 
public String getCronName() {
        return cron_name;
}
 
 
public void setInterval(int value) {
        interval = value;
}
 
public String getInterval() {
        return "" + interval;
}
 
public void setEnableState(boolean value) {
        isEnabled = value;
}
 
public boolean getEnableState() {
        return isEnabled;
}

public void consumerScheduler(){
	/*try{			
		String url = "http://twitter-trendsanalyzer.mybluemix.net/KafkaConsumer";
		HttpClient client = HttpClientBuilder.create().build();
		HttpGet request = new HttpGet(url);
		request.addHeader("User-Agent", "");
		HttpResponse response = client.execute(request);
		System.out.println("Response status"+response.getStatusLine().getStatusCode());
	}catch(Exception e){System.out.println(e);
		//logger.error(e);
	}*/
	trendsFinder();
	
}


public void trendsFinder(){
	JsonObject responseJson=null;
	//JsonElement je=null;
	//String trends=null;
	//String trendsRetrived[]=new String[5];
	try{
		MetaKeywordsDAO metaKeywordsDAO = new MetaKeywordsDAO();
		List<String> trendsData=metaKeywordsDAO.getTrendsDiscoveryData();
		
		System.out.println("trendsData iterator::"+trendsData.size());
		Iterator<String> iterator = trendsData.iterator();
		while (iterator.hasNext()) {
				//System.out.println(iterator.next());
				String mikSubclass=iterator.next();
				System.out.println("mikSubclass value::"+mikSubclass);	
				//String url = "http://api.walmartlabs.com/v1/search?apiKey=agevmwa5rhme979szegdj3v6&query=PHOTO%20SHADOW%20BOX%20TRAY&sort=customerRating&order=desc&numItems=5";
				String url = "http://api.walmartlabs.com/v1/search?apiKey=agevmwa5rhme979szegdj3v6&query="+mikSubclass+"&sort=customerRating&order=desc&numItems=5";
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
					JsonElement je=jsonArray.get(i);			
					JsonParser parser1 = new JsonParser();
					responseJson = parser1.parse(je.toString()).getAsJsonObject();
					String trends=responseJson.get("categoryPath").getAsString();
					trends=trends.substring(trends.lastIndexOf("/")+1);
					
					System.out.println("trends #:"+i+"::"+trends);
					
					//trendsRetrived[i]=trends;
					
					MetaKeywords metaKeyWords=new MetaKeywords();
					metaKeyWords.setKeywordName(trends);
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
		
		
		//System.out.println("Response status"+response.getStatusLine().getStatusCode());
		//System.out.println("Response from wallmart api:##"+response.toString());
	}catch(Exception e){System.out.println(e);
		//logger.error(e);
	}
	//return trendsRetrived[0]+":"+trendsRetrived[1]+"::"+trends;
}

}
