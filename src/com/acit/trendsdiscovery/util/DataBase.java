package com.acit.trendsdiscovery.util;

import java.sql.Connection;

import java.sql.SQLException;
import java.util.Set;

import com.ibm.db2.jcc.DB2SimpleDataSource;
import com.ibm.nosql.json.api.BasicDBList;
import com.ibm.nosql.json.api.BasicDBObject;
import com.ibm.nosql.json.util.JSON;

/****
 * 
 * Database access module
 *
 */
public class DataBase {
	
	// set defaults
		private static String databaseHost = "";
		private static int port =0;
		private static String databaseName = "";
		private static String user = "";
		private static String password = "";
		private static String url = "";
		private static String tport = "";		
		boolean flag;		
		public static DataBase dataBase;
		public static Connection connection = null;
		public static DB2SimpleDataSource dataSource = new DB2SimpleDataSource();

		/****
		 * Assign DB connection parms from VCAP variables
		 */
		private DataBase(){
			if (processVCAP()) {  			
	  			try {	  				
	  				dataSource.setServerName(databaseHost);
	  				dataSource.setPortNumber(port);
	  				dataSource.setDatabaseName(databaseName);
	  				dataSource.setUser(user);
	  				dataSource.setPassword(password);
	  				dataSource.setDriverType(4);
	  				//con = dataSource.getConnection();
	  				//con.setAutoCommit(false);
	  			} catch (Exception e) {
	  				//return con;
	  			}  			
	    	  }
		}
		
		public static synchronized DataBase getInstance() {
	        if ( dataBase == null ) {
	        	dataBase = new DataBase();
	        }
	        return dataBase;
	 
	    }
		
		/***
		 * Get connection
		 * @return
		 */
		public Connection getConnection(){
		    try{
		    	connection = dataSource.getConnection();
		    	if ((connection == null) || connection.isClosed() ) {
		    		connection = dataSource.getConnection();
  				}
		    }
		  
		    catch (SQLException e1){
		      e1.printStackTrace();
		    }		     		    		     
		    return connection;   		  
		}
		
       public static void close(Connection con) {
          try  {
              con.close();
          }
          catch(Exception ex) {
          }
      }
       
       private static boolean processVCAP() {
   		// VCAP_SERVICES is a system environment variable
   		// Parse it to obtain the for DB2 connection info
   		String VCAP_SERVICES = System.getenv("VCAP_SERVICES");
   		System.out.println("VCAP_SERVICES");
   		if (VCAP_SERVICES != null) {//System.out.println("VCAP_SERVICES###");
   			// parse the VCAP JSON structure
   			BasicDBObject obj = (BasicDBObject) JSON.parse(VCAP_SERVICES);
   			String thekey = null;
   			Set<String> keys = obj.keySet();
   			// Look for the VCAP key that holds the SQLDB information
   			for (String eachkey : keys) {
   				if (eachkey.toUpperCase().contains("DASHDB")) {
   					thekey = eachkey;
   				}
   			}
   			if (thekey == null) {
   				return false;
   			}
   			BasicDBList list = (BasicDBList) obj.get(thekey);
   			obj = (BasicDBObject) list.get("0");
   			// parse all the credentials from the vcap env variable 			   			
   			obj = (BasicDBObject) obj.get("credentials");
   			databaseHost = (String) obj.get("host");
   			databaseName = (String) obj.get("db");
   			tport = obj.get("port").toString();
   			port = Integer.parseInt(tport);
   			user = (String) obj.get("username");
   			password = (String) obj.get("password");
   			url = (String) obj.get("jdbcurl");
   			
   		} else {
   			return false;
   		}   		
   		return true;
   	} 
}