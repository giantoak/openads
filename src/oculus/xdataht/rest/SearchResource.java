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

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import oculus.aperture.common.rest.ApertureServerResource;
import oculus.xdataht.clustering.LinkFilter;
import oculus.xdataht.data.DataRow;
import oculus.xdataht.data.DataUtil;
import oculus.xdataht.data.SearchCache;
import oculus.xdataht.data.TableDB;
import oculus.xdataht.model.ClusterDetailsResult;
import oculus.xdataht.model.RestFilter;
import oculus.xdataht.model.SearchRequest;
import oculus.xdataht.model.StringMap;

import org.restlet.resource.ResourceException;

@Path("/search")
public class SearchResource extends ApertureServerResource {
	@POST
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	@Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
	public ClusterDetailsResult handlePost(SearchRequest request) throws ResourceException {
		ArrayList<StringMap> serializableDetails = new ArrayList<StringMap>();
		
		String datasetName = request.getDatasetName();
		String outputName = request.getOutputName();
		
		ArrayList<RestFilter> restFilters = request.getFilters();
		if (restFilters == null) {
			restFilters = new ArrayList<RestFilter>();
		}
		String where = SearchResource.getWhereClause(restFilters);
		
		TableDB db = TableDB.getInstance();
		db.open();
		try {
			ArrayList<DataRow> matches = db.getMatches(datasetName, where);
			ArrayList<HashMap<String, String>> rows = DataUtil.sanitizeHtml(matches);
			for (HashMap<String, String> row:rows) {
				serializableDetails.add( new StringMap(row));
			}
			
			// TODO: handle the search cache.
			SearchCache.setResults(outputName, matches);
		} catch (Exception e) {
			e.printStackTrace();
		}
		db.close();
		
		return new ClusterDetailsResult(serializableDetails);
	}
	
	public static String getWhereClause(ArrayList<RestFilter> filters) {
		String where = "where ";
		boolean isFirst = true;
		for (RestFilter restfilter:filters) {
			LinkFilter filter = new LinkFilter(restfilter);
			if (isFirst) isFirst = false;
			else where += " AND ";
			where += filter.getWhereClause();
		}
		return where;
	}	
}