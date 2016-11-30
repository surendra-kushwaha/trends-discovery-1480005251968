/**
 * 
 */
package com.acit.trendsdiscovery.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;

import org.apache.wink.json4j.JSONArray;
import org.apache.wink.json4j.JSONObject;

/**
 * @author indira
 *
 */

public class SemrushClient {
		
	public InputStreamReader getTrendsFromSemrushAPI(String searchKeyword, String export_columns){
	
		System.out.println("In getTrendsFromSemrushAPI ..");
		String keyword = searchKeyword.replace(" ", "+");
		
		System.out.println("Search Keyword : "+keyword);
		
		try{
			//System.getenv("semrushAPIEndpoint")
		URL url = new URL("http://api.semrush.com/"
				+ "?type=url_organic"
				//+ "&phrase="+keyword //search keyword
				+ "&url="+keyword //search keyword
				//+ "&key=" + System.getenv("semrushApiKey") //semrush API key(key=20230bcb09669464b97530a41e8e32c3)
				+ "&key=20230bcb09669464b97530a41e8e32c3" //semrush API key(key=20230bcb09669464b97530a41e8e32c3)
				+ "&display_limit=10"  //retrieve 10 records
				+ "&export_columns="+export_columns  //columns to retrieve
				+ "&database=us");  //currently limited to US DB
		
		HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
		InputStreamReader in = new InputStreamReader(
				(urlConnection.getInputStream()));
		
		return in;
		
		}catch(Exception e ){
			
			System.out.println("Exception in getResultsFromSemrushAPI "+e);
		}
		
		return null;
	}
	

	public JSONObject processKeywordResults(String searchKeyword, String export_columns , InputStreamReader in){	
		
		System.out.println("In SemrushClient -processKeywordResults ");
		try
		{
			String keyword = searchKeyword.replace(" ", "+");
			
			System.out.println("Search Keyword : "+keyword);
			
			String[] exportColumnsList= export_columns.split(",");
			HashMap< Integer, String> columnIndex  = new HashMap<Integer, String>();
			
			for(int i=0; i<exportColumnsList.length; i++){
				columnIndex.put(i,exportColumnsList[i]);
			}

			System.out.println("Resultant keywords from semrush :: ");
			
			JSONObject semrushResult = new JSONObject();
			semrushResult = new JSONObject();
			semrushResult.put("Topic", searchKeyword);
			
			JSONArray semrushKeyWordArray = new JSONArray();
			
			String result ="";
			
			BufferedReader br = new BufferedReader(in);
			if((result = br.readLine()) != null) 
				System.out.println("Header : " +result);
				
			while ((result = br.readLine()) != null) {
				
				JSONObject semrushKeyword = new JSONObject();

				System.out.println(result);
				
				String [] keywords = result.split(";");
				semrushKeyword = new JSONObject();

				for(int i=0; i <keywords.length; i++){
					
					switch (columnIndex.get(i)) {
					
						case "Ph":
									semrushKeyword.put("Keyword", keywords[i]);
									break;
						
						case "Nq":
									semrushKeyword.put("Search Volume", keywords[i]);
									break;
						case "Cp":
									semrushKeyword.put("CPC", keywords[i]);
									break;
						case "Co":
									semrushKeyword.put("Competition", keywords[i]);							
									break;
						case "Nr":
									semrushKeyword.put("Number of Results", keywords[i]);							
									break;
						case "Td":
									semrushKeyword.put("Trends", keywords[i]);							
									break;
						default:
							break;
					}
				}
				
				semrushKeyWordArray.add(semrushKeyword);
				}
				
				semrushResult.put("result", semrushKeyWordArray);
				System.out.println("Keyword Result JSON : "+semrushResult);
				
				br.close();
				
				return semrushResult;
			
		}catch(Exception e ){
			e.printStackTrace();
		}
		return null;
	}
}
