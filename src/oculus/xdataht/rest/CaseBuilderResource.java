package oculus.xdataht.rest;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import oculus.xdataht.attributes.AttributeDetails;
import oculus.xdataht.casebuilder.CaseBuilder;
import oculus.xdataht.data.DataUtil;
import oculus.xdataht.data.TableDB;
import oculus.xdataht.util.TimeLog;

import org.json.JSONException;
import org.json.JSONObject;

@Path("/casebuilder")
public class CaseBuilderResource {

	@Context
	UriInfo _uri;

	@POST
	@Path("savecase")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	@Consumes({MediaType.TEXT_HTML, MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public String saveCase(String caseAdditions){
		TimeLog log = new TimeLog();
		log.pushTime("Save Case: ");
		JSONObject jo = null;
		TableDB db = TableDB.getInstance();
		try {
			jo = new JSONObject(caseAdditions);
			String[] cases = JSONObject.getNames(jo);
			PreparedStatement pstmt = null;
			Connection conn = db.open();
			for(String caseName: cases) {
				pstmt = conn.prepareStatement("INSERT IGNORE INTO " + CaseBuilder.CASE_BUILDER_TABLE + 
						" (id, is_attribute, case_name) VALUES (?,?,?)");
				JSONObject all = jo.getJSONObject(caseName);
				String attrs = all.getString("true");
				String clust = all.getString("false");
				if(attrs.length()>0) {
					String[] attributes = attrs.split(",");
					for(int i = 0; i<attributes.length; i++) {
						pstmt.setInt(1, Integer.parseInt(attributes[i]));
						pstmt.setBoolean(2, true);
						pstmt.setString(3, caseName);
						pstmt.addBatch();
					}
				}
				if(clust.length()>0) {
					String[] clusters = clust.split(",");
					for(int i = 0; i<clusters.length; i++) {
						pstmt.setInt(1, Integer.parseInt(clusters[i]));
						pstmt.setBoolean(2, false);
						pstmt.setString(3, caseName);
						pstmt.addBatch();
					}
				}
				pstmt.executeBatch();
			}
			pstmt.close();
			JSONObject result = new JSONObject();
			result.put("Success", "true");
			for(String caseName: cases) {
				result.put(caseName, Integer.toString(getCaseSize(db, conn, caseName)));				
			}
			db.close();	
			log.popTime();
			return result.toString();
		} catch (JSONException e) {
			e.printStackTrace();
			db.close();
			return "{\"Success\": \"JSON parsing Error\"}";
		} catch (SQLException e) {
			e.printStackTrace();
			db.close();
			return "{\"Success\": \"SQL Error\"}";
		}		
	}
	
	@POST
	@Path("deletenode")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	@Consumes({MediaType.TEXT_HTML, MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public String deleteNode(String node){
		TimeLog log = new TimeLog();
		log.pushTime("Delete Node: " + node);
		TableDB db = TableDB.getInstance();
		Connection conn = db.open();
		PreparedStatement pstmt;
		JSONObject jo;
		try {
			jo = new JSONObject(node);
			String caseName = jo.getString("case_name");
			pstmt = conn.prepareStatement("DELETE FROM " + CaseBuilder.CASE_BUILDER_TABLE + 
					" WHERE case_name=? AND id=? AND is_attribute=?");
			pstmt.setString(1, caseName);
			pstmt.setInt(2, jo.getInt("id"));
			pstmt.setBoolean(3, jo.getBoolean("is_attribute"));
			int result = pstmt.executeUpdate();
			int count = getCaseSize(db, conn, caseName);
			db.close();
			log.popTime();
			if(result!=-1)
				return "{\"Success\": \"true\", \"Count\": \"" + count + "\"}";
			return "{\"Success\": \"false\"}";
		} catch (JSONException e) {
			e.printStackTrace();
			db.close();
			return "{\"Success\": \"JSON parsing Error\"}";
		} catch (SQLException e) {
			e.printStackTrace();
			db.close();
			return "{\"Success\": \"SQL Error\"}";
		}
	}	
	
	private int getCaseSize(TableDB db, Connection conn, String caseName) {
		String sqlStr = "SELECT COUNT(*) AS count FROM " + CaseBuilder.CASE_BUILDER_TABLE + 
				" WHERE case_name='" + caseName + "'";
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			if(rs.next())
				return rs.getInt("count");
			return -1;
		} catch (SQLException e) {
			e.printStackTrace();
			return -2;
		}
	}

	@POST
	@Path("getcase")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	@Consumes({MediaType.TEXT_HTML, MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public String getCase(String caseName){
		caseName = DataUtil.sanitizeHtml(caseName);
		TimeLog log = new TimeLog();
		log.pushTime("Get case");
		TableDB db = TableDB.getInstance();
		Connection conn = db.open();
		String sqlStr = "SELECT is_attribute, id FROM " + CaseBuilder.CASE_BUILDER_TABLE +
				" WHERE case_name='" + caseName + "'";
		Statement stmt = null;
		StringBuilder attributes = new StringBuilder();
		StringBuilder clusters = new StringBuilder();
		attributes.append("(");
		clusters.append("(");
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			while(rs.next()) {
				if(rs.getBoolean("is_attribute")) {
					attributes.append(rs.getInt("id") + ",");
				} else {
					clusters.append(rs.getInt("id") + ",");
				}
			}
			JSONObject records = new JSONObject();
			ArrayList<JSONObject> nodes = new ArrayList<JSONObject>();
			if(clusters.length()>1) {
				sqlStr = "SELECT clusterid, adcount, clustername FROM " + TableDB.CLUSTER_DETAILS_TABLE + 
						" WHERE clusterid IN " + clusters.substring(0, (clusters.length()-1)) + ")";
				rs = stmt.executeQuery(sqlStr);			
				while(rs.next()) {
					JSONObject record = new JSONObject();
					record.put("ATTRIBUTE_MODE", "false");
					record.put("Cluster Size", rs.getString("adcount"));
					record.put("label", rs.getString("clustername"));
					record.put("id", rs.getString("clusterid"));
					nodes.add(record);
				}
			}
			if(attributes.length()>1) {
				sqlStr = "SELECT attributes_id, adcount, clustername FROM " + AttributeDetails.ATTRIBUTES_DETAILS_TABLE + 
						" WHERE attributes_id IN " + attributes.substring(0, (attributes.length()-1)) + ")";
				rs = stmt.executeQuery(sqlStr);			
				while(rs.next()) {
					JSONObject record = new JSONObject();
					record.put("ATTRIBUTE_MODE", "true");
					record.put("Cluster Size", rs.getString("adcount"));
					record.put("label", rs.getString("clustername"));
					record.put("id", rs.getString("attributes_id"));
					nodes.add(record);
				}
			}
			stmt.close();
			db.close();
			log.popTime();
			if(nodes.isEmpty()) {
				return "{\"Success\": \"'"+ caseName + "' does not exist\"}";
			} 
			records.put(caseName, nodes.toArray());
			return records.toString();
		} catch (SQLException e) {
			e.printStackTrace();
			db.close();
			return "{\"Success\": \"SQL Error\"}";
		} catch (JSONException e) {
			e.printStackTrace();
			db.close();
			return "{\"Success\": \"JSON Error\"}";
		} finally {
			try {
				if (stmt != null) { stmt.close(); }
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	@GET
	@Path("getnames")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public String getNames(){
		TimeLog log = new TimeLog();
		log.pushTime("Get case names");
		StringBuilder result = new StringBuilder();
		TableDB db = TableDB.getInstance();
		Connection conn = db.open();
		String sqlStr = "SELECT DISTINCT case_name FROM " + CaseBuilder.CASE_BUILDER_TABLE;
		Statement stmt;
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			result.append("{\"caseNames\": [");
			boolean first = true;
			while(rs.next()) {
				if(first) {
					result.append("\"" + rs.getString("case_name")+"\"");
					first = false;
				} else {
					result.append(",\"" + rs.getString("case_name")+"\"");
				}
			}
			stmt.close();
			db.close();
			log.popTime();			
			return result + "]}";
		} catch (SQLException e) {
			e.printStackTrace();
			return "{\"Success\": \"SQL Error\"}";
		}
	}
	
	@POST
	@Path("getname")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	@Consumes({MediaType.TEXT_HTML, MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public String getName(String name){
		TimeLog log = new TimeLog();
		log.pushTime("Case getName: " + name);
		TableDB db = TableDB.getInstance();
		Connection conn = db.open();
		String result = name;
		int ver = 2;
		while(caseExists(result, db, conn)){
			result=name+"_"+ver;
			ver++;
		}
		db.close();
		log.popTime();
		return "{\"Name\": \"" + result +"\"}";		
	}
	
	/** 
	 * @param name The case name being tested.
	 * @param conn 
	 * @param db 
	 * @return True if there exists a case with the String name provided.
	 */
	private boolean caseExists(String name, TableDB db, Connection conn) {
		String sqlStr = "SELECT * FROM " + CaseBuilder.CASE_BUILDER_TABLE + 
				" WHERE case_name='" + name + "' LIMIT 1";
		Statement stmt;
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlStr);
			boolean result = rs.next();
			stmt.close();
			return result;			
		} catch (SQLException e) {
			e.printStackTrace();
			return true;
		}
	}
}