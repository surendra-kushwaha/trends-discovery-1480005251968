/**
 * 
 */
package com.acit.trendsdiscovery.util;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.wink.json4j.JSONArray;
import org.apache.wink.json4j.JSONObject;

import com.acit.trendsdiscovery.dao.MetaKeywordsDAO;

/**
 * Topic Association logic
 * @author indira
 *
 */
public class TopicAssociationGraphdb {
	private static final Logger log = Logger.getLogger(TopicAssociationGraphdb.class.getName());

	private GraphRESTUtil graphUtil = null;
	
	MetaKeywordsDAO metaKeywordsDAO = new MetaKeywordsDAO();
	
	public TopicAssociationGraphdb() {
		initialize();
	}

	private void initialize() {
		graphUtil = new GraphRESTUtil();
	}

	
	/****
	 * Retrieve all categories (top level) from graph DB 
	 * from the product hierarchy
	 * @return
	 */
	// Get all Categories From GraphDB - current not being used. Have to look at gremlin query
	
	public JSONObject getAllCategories() {

		log.info("In TopicAssociationGraphdb  getAllCategories >>>> ");
		JSONObject result = null;
		try 
		{

			// label 'C1' below denotes top level or first level categories in the product hierarchy
			JSONObject postData = new JSONObject();
			postData.put("gremlin", "def gt = graph.traversal(); gt.V().has('C1', 'type','C')");
			StringEntity stringEntity = new StringEntity(postData.toString(), ContentType.APPLICATION_JSON);

			// Invoke Graph utility and retrieve the response
			String content = graphUtil.invokeGraphPost(stringEntity);
			JSONObject jsonContent = new JSONObject(content);
			log.info("Graphdb Response : " + jsonContent);

			if(jsonContent.has("result")){
				result = jsonContent.getJSONObject("result");
				log.info(result.toString());
			}

		} catch (Exception e) {
			log.log(Level.SEVERE, "Unable to fetch all categories from graphDB -> " + e.getMessage(), e);
			
		}
		return result;
	}

	/****
	 * Retrieve all categories from Graph DB
	 * @return
	 */
	// Get Categories From GraphDB
	public JSONObject getCategories() {
		System.out.println(" getcategory call ");
		log.info("In TopicAssociationGraphdb  getCategories >>>> ");
		JSONObject result = null;
		
		try {
			
			// type 'C' below denotes categories in the product hierarchy
			JSONObject postData = new JSONObject();
			postData.put("gremlin", "def gt = graph.traversal();"
					+ " gt.V().has('department','type','C')");
			StringEntity stringEntity = new StringEntity(postData.toString(), ContentType.APPLICATION_JSON);

			// Invoke Graph utility and retrieve the response
			String content = graphUtil.invokeGraphPost(stringEntity);
			JSONObject jsonContent = new JSONObject(content);
			log.info("Graphdb Response : " + jsonContent);
			if(jsonContent.has("result")){
				result = jsonContent.getJSONObject("result");
				log.info(result.toString());
			}

		} catch (Exception e) {
			log.log(Level.SEVERE, "Unable to fetch categories from graphDB -> " + e.getMessage(), e);
			log.info("Unable to fetch categories from graphDB ");
			e.printStackTrace();
		}
		return result;
	}

	/****
	 * Retrieve sub-categories of a given category
	 * from graph DB in the product hierarchy
	 * @param categoryName
	 * @return
	 */
	// Get getSubCategoriesOfACategory From GraphDB
		public JSONObject getSubCategoriesOfACategory(String categoryName) {

			log.info("In TopicAssociationGraphdb  getSubCategoriesOfACategory >>>> ");
			JSONObject subCategory = new JSONObject();
			
			try {

				//has relation compares the existence of category
				//type 'C' denotes the type in graph DB
				JSONObject postData = new JSONObject();
				postData.put("gremlin", "def gt = graph.traversal(); gt.V().has('name','" + categoryName + "')"
						+ ".out('contains').has('type','C')");
				StringEntity stringEntity = new StringEntity(postData.toString(), ContentType.APPLICATION_JSON);

				// Invoke Graph utility and retrieve the response
				String content = graphUtil.invokeGraphPost(stringEntity);
				JSONObject jsonContent = new JSONObject(content);
				log.info("Graphdb Response : " + jsonContent);
				
				JSONObject result = null;
				if(jsonContent.has("result")){
					
					result = jsonContent.getJSONObject("result");
					if(result != null && result.has("data")){
						
						JSONArray dataArray  = result.getJSONArray("data");
						
						subCategory.put("categoryName", categoryName);
						subCategory.put("subCaegories", getSubItemsFromResult(dataArray));
					}
				}
				
				log.info(subCategory.toString());

			} catch (Exception e) {
				log.log(Level.SEVERE, "Unable to fetch sub categories from graphDB -> " + e.getMessage(), e);
				log.info("Unable to fetch sub categories from graphDB ");
			}

			return subCategory;
		}
		
		/***
		 * JSON operations - retrieve items from the result JSON array
		 * @param data
		 * @return
		 */
		public JSONArray getSubItemsFromResult(JSONArray data){
			log.info("In TopicAssociationGraphdb  getSubItemsFromResult >>>> ");
			JSONArray catergoryList = new JSONArray();
			try{
					for (Object obj : data) {
					    
						//JSONObject categories = (JSONObject)obj;
						//JSONArray nodes = categories.getJSONArray("objects");
						
						//Child Node Object
						JSONObject node = (JSONObject)obj;
						String topicNodeID = node.getString("id");
						
						//log.info("S Node TopicNode ID : "+topicNodeID);
						JSONObject nodeProps = (JSONObject)node.get("properties");
						String topicName = ((JSONObject)((JSONArray) nodeProps.getJSONArray("name")).get(0)).get("value").toString();
						String type = ((JSONObject)((JSONArray) nodeProps.getJSONArray("type")).get(0)).get("value").toString();
						
						JSONObject categoryNode = new JSONObject();
						categoryNode.put("topicName", topicName);
						categoryNode.put("id", topicNodeID);
						categoryNode.put("type", type);
						
						catergoryList.add(categoryNode);
				
					}
				
			}catch(Exception e){
				e.printStackTrace();
			}
				
			return catergoryList;
		}


}
