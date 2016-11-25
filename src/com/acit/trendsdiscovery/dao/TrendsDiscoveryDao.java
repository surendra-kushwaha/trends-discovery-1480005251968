package com.acit.trendsdiscovery.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


import com.acit.trendsdiscovery.model.TrendInfo;
import com.acit.trendsdiscovery.util.DataBase;

/****
 * 
 * Twitter trends analyzer data access object
 * includes updates to TOPIC, TWITTER TOPICS and TOPIC trends table
 * and determination of trend variation
 *
 */
public class TrendsDiscoveryDao {

	private Connection connection;

	/***
	 * Initialize DB connection object
	 */
	public TrendsDiscoveryDao() {
		connection = DataBase.getInstance().getConnection();
	}

	/***
	 * Add new topics into TOPIC table for Twitter Topics are validated if
	 * already exists
	 * 
	 * @param topicName
	 * @return
	 */
	// *** addTopic method updated by Surendra ****** //
	public boolean addTopic(String topicName) {

		boolean updateSuccessFlag = false;
		PreparedStatement preparedStatement = null;
		try {
			if ((connection == null) || connection.isClosed()) {
				connection = DataBase.getInstance().getConnection();
			}
			System.out.println("addTopic" + topicName);
			if (!validateTopic(topicName)) {
				System.out.println("topicname not exists");
				StringBuffer queryString = new StringBuffer();
				queryString.append(System.getenv("SQL_INSERT_TOPIC"));
				preparedStatement = connection.prepareStatement(queryString.toString());
				preparedStatement.setString(1, topicName);
				int updateflag = preparedStatement.executeUpdate();
				if (updateflag > 0) {
					updateSuccessFlag = true;
				}
			}
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			close(null, preparedStatement, null);
		}
		return updateSuccessFlag;
	}

	/****
	 * Check if the topic exists already
	 * 
	 * @param topicName
	 * @return
	 */
	public boolean validateTopic(String topicName) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		boolean topicAvailable = false;
		List<Integer> topicIdList = new ArrayList<Integer>();

		try {
			if ((connection == null) || connection.isClosed()) {
				connection = DataBase.getInstance().getConnection();
			}
			ps = connection.prepareStatement(System.getenv("SQL_SELECT_TOPIC"));
			ps.setString(1, topicName);
			rs = ps.executeQuery();
			while (rs.next()) {
				topicIdList.add(rs.getInt("TOPIC_ID"));
				topicAvailable = true;
			}
		} catch (Exception ex) {
			System.out.println("Error in check() -->" + ex.getMessage());
		} finally {
			close(rs, ps, null);
		}
		System.out.println("topicIdList size##" + topicIdList);
		return topicAvailable;
	}
	/****
	 * Close result set, prepared statement, connection
	 * @param rs
	 * @param pstmt
	 * @param conn
	 */
	public void close(ResultSet rs, PreparedStatement pstmt, Connection conn) {
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
				System.out.println("The result set cannot be closed." + e);
			}
		}
		if (pstmt != null) {
			try {
				pstmt.close();
			} catch (SQLException e) {
				System.out.println("The prepared statement cannot be closed." + e);
			}
		}
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				System.out.println("The data source connection cannot be closed." + e);
			}
		}

	}

	/****
	 * Override method
	 * @param rs
	 * @param pstmt
	 */
	public void close(ResultSet rs, PreparedStatement pstmt) {
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
				System.out.println("The result set cannot be closed." + e);
			}
		} else if (pstmt != null) {
			try {
				pstmt.close();
			} catch (SQLException e) {
				System.out.println("The prepared statement cannot be closed." + e);
			}
		}
	}
} // End Trend Analyzer for Twitter
