/**
 * Copyright (c) 2013 Oculus Info Inc.
 * http://www.oculusinfo.com/
 *
 * Released under the MIT License.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is furnished to do
 * so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package oculus.memex.rest;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import oculus.memex.clustering.AttributeDetails;
import oculus.memex.clustering.AttributeValue;
import oculus.memex.db.MemexHTDB;
import oculus.memex.db.MemexOculusDB;
import oculus.memex.graph.AttributeLinks;
import oculus.xdataht.data.DataRow;
import oculus.xdataht.data.DataUtil;
import oculus.xdataht.model.ClusterDetailsResult;
import oculus.xdataht.model.ClustersDetailsResult;
import oculus.xdataht.model.StringMap;
import oculus.xdataht.util.TimeLog;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

@Path("/attributeDetails")
public class AttributeDetailsResource  {
	@Context
	UriInfo _uri;
	
	@GET
	@Path("{attribute}/{value}")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public ClusterDetailsResult handleGet(@PathParam("attribute")String attribute, @PathParam("value")String value, @Context HttpServletRequest request) {
		List<DataRow> results = new ArrayList<DataRow>();
		TimeLog log = new TimeLog();
		log.pushTime("Attribute details: " + attribute + ":" + value);
		log.pushTime("Fetch Ad IDs");
		// Open both databases
		MemexOculusDB oculusdb = MemexOculusDB.getInstance();
		Connection oculusconn = oculusdb.open();

		MemexHTDB htdb = MemexHTDB.getInstance();
		Connection htconn = htdb.open();

		Integer attrid = null;
		HashMap<Integer,AttributeValue> allAttributes = AttributeLinks.getAttributes(oculusconn);
		if (attribute.equals("id")) {
			attrid = Integer.parseInt(value);
		} else {
			for (Entry<Integer,AttributeValue> e:allAttributes.entrySet()) {
				AttributeValue av = e.getValue();
				if (av.attribute.equals(attribute) && av.value.equals(value)) {
					attrid = e.getKey();
					break;
				}
			}
		}
		
		if (attrid==null) {
			oculusdb.close();
			htdb.close();
			log.popTime();
			log.popTime();
			return null;
		}
		
		// Get the ad->attribute list mapping
		HashMap<Integer,HashSet<Integer>> adToAttributes = new HashMap<Integer,HashSet<Integer>>();
		ArrayList<Integer> ads = new ArrayList<Integer>();
		AttributeDetails.getAdsInAttributes(attrid, attrid, allAttributes, adToAttributes, oculusconn, htconn, ads);
		HashSet<Integer> members = new HashSet<Integer>(adToAttributes.keySet());
		
		oculusdb.close();
		htdb.close();
		
		log.popTime();
		log.pushTime("Fetch Ad Contents");
		PreclusterDetailsResource.getDetails(members, results, request.getRemoteUser());
		log.popTime();

		log.pushTime("Prepare results");

		ArrayList<HashMap<String,String>> details = DataUtil.sanitizeHtml(results);

		ArrayList<StringMap> serializableDetails = new ArrayList<StringMap>();
		for (HashMap<String,String> map : details) {
			serializableDetails.add( new StringMap(map));
		}
		log.popTime();
		log.popTime();
		return new ClusterDetailsResult(serializableDetails);		
	}
	
	/**
	 * 
	 * @param attributesIds
	 * @param request
	 * @return Map of attributeid --> ClusterDetailsResource
	 */
	@POST
	@Path("fetchAds")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	@Consumes({MediaType.TEXT_HTML, MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public ClustersDetailsResult fetchAds(String attributesIds, @Context HttpServletRequest request) {
		TimeLog log = new TimeLog();
		log.pushTime("Fetching attribute graph ad details for user " + request.getRemoteUser());
		HashMap<Integer, ClusterDetailsResult> results = new HashMap<Integer, ClusterDetailsResult>();
		MemexOculusDB oculusdb = MemexOculusDB.getInstance();
		MemexHTDB htdb = MemexHTDB.getInstance();
		Connection oculusconn;
		Connection htconn;

		try {
			JSONObject jo = new JSONObject(attributesIds);
			JSONArray attributeClusterIds = jo.getJSONArray("ids");
			oculusconn = oculusdb.open();
			HashMap<Integer,AttributeValue> allAttributes = AttributeLinks.getAttributes(oculusconn);
			oculusdb.close();
			for(int i = 0;i<attributeClusterIds.length();i++) {
				Integer attrid = Integer.parseInt(attributeClusterIds.get(i).toString());

				// Get the ad->attribute list mapping
				log.pushTime("Fetch Ad IDs for cluster " + attrid);
				HashMap<Integer,HashSet<Integer>> adToAttributes = new HashMap<Integer,HashSet<Integer>>();
				ArrayList<Integer> ads = new ArrayList<Integer>();
				htconn = htdb.open();
				oculusconn = oculusdb.open();
				AttributeDetails.getAdsInAttributes(attrid, attrid, allAttributes, adToAttributes, oculusconn, htconn, ads);
				htdb.close();
				oculusdb.close();
				HashSet<Integer> members = new HashSet<Integer>(adToAttributes.keySet());
				log.popTime();
				
				log.pushTime("Fetch ad contents for cluster " + attrid);
				List<DataRow> result = new ArrayList<DataRow>();
				PreclusterDetailsResource.getDetails(members, result, request.getRemoteUser());
				log.popTime();

				log.pushTime("Prepare results for attribute cluster: " + attrid);
				ArrayList<HashMap<String,String>> details = DataUtil.sanitizeHtml(result);
				ArrayList<StringMap> serializableDetails = new ArrayList<StringMap>();
				for (HashMap<String,String> map : details) {
					serializableDetails.add( new StringMap(map));
				}
				results.put(attrid, new ClusterDetailsResult(serializableDetails));
				log.popTime();
			}			
			log.popTime();
			return new ClustersDetailsResult(results);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	
	@GET
	@Path("getattrid/{attribute}/{value}")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	@Consumes({MediaType.TEXT_HTML, MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public String getAttributeId(@PathParam("attribute")String attribute, @PathParam("value")String value) {
		TimeLog log = new TimeLog();
		log.pushTime("Fetching attribute id for attribute " + attribute + ", value " + value);
		String result = null;
		MemexOculusDB db = MemexOculusDB.getInstance();
		Connection conn = db.open();
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT id FROM " + AttributeDetails.ATTRIBUTE_DETAILS_TABLE + " WHERE attribute='"+attribute+"' AND value='"+value+"'");
			rs.next();
			result = rs.getString("id");
		} catch (Exception e) {
			e.printStackTrace();
		}
		db.close();
		log.popTime();
		return result;
	}
}