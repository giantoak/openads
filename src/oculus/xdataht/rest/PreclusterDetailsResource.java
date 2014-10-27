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
package oculus.xdataht.rest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import oculus.xdataht.data.DataRow;
import oculus.xdataht.data.DataTable;
import oculus.xdataht.data.DataUtil;
import oculus.xdataht.data.TableDB;
import oculus.xdataht.model.ClusterDetailsResult;
import oculus.xdataht.model.StringMap;
import oculus.xdataht.util.TimeLog;

@Path("/preclusterDetails")
public class PreclusterDetailsResource  {
	@Context
	UriInfo _uri;
	
	@GET
	@Path("{preclusterType}/{clusterId}")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public ClusterDetailsResult handleGet(@PathParam("preclusterType") String preclusterType, @PathParam("clusterId") String clusterId) {
		List<DataRow> results = new ArrayList<DataRow>();
		
		TimeLog log = new TimeLog();
		log.pushTime("Precluster details: " + preclusterType + ":" + clusterId);
		log.pushTime("Fetch Ad IDs");
		Set<String> members = TableDB.getInstance().getPreclusterAds(preclusterType, clusterId);
		log.popTime();
		log.pushTime("Fetch Ad Contents");
		DataTable table = TableDB.getInstance().getDataTableMembers("ads", members);
		log.popTime();

		log.pushTime("Prepare results");
		for (String memberId : members) {
			DataRow row = table.getRowById(memberId);
			if (row != null) {
				
				// Add any user tags to the row
				ArrayList<String> tags = TableDB.getInstance().getTags(memberId);
				String tagString = "";
				if (tags != null && tags.size() > 0) {
					for (int i = 0; i < tags.size() - 1; i++) {
						tagString += tags.get(i) + ',';
					}
					tagString += tags.get(tags.size()-1);
				}
				row.put("tags", tagString);
				results.add(row);
			}
		}

		ArrayList<HashMap<String,String>> details = DataUtil.sanitizeHtml(results);

		ArrayList<StringMap> serializableDetails = new ArrayList<StringMap>();
		for (HashMap<String,String> map : details) {
			serializableDetails.add( new StringMap(map));
		}
		log.popTime();
		log.popTime();
		return new ClusterDetailsResult(serializableDetails);		
	}

}
