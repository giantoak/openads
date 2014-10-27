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

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import oculus.xdataht.data.TableDB;
import oculus.xdataht.model.StringList;
import oculus.xdataht.model.TagsResult;
import oculus.xdataht.model.UpdateTagRequest;

@Path("/tags")
public class TagsResource {
	
	@POST
	@Path("fetch")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	@Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
	public TagsResult fetch(StringList adIds) {
		return new TagsResult( TableDB.getInstance().getTags(adIds.getList()) );
	}
	
	@POST
	@Path("update")
	@Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
	public void update(UpdateTagRequest request) {
		ArrayList<String> tags = request.getTags();
		if (tags != null && tags.size() > 0) {
			if (request.getAdd() == true) {
				TableDB.getInstance().addTags(request.getAdIds(), tags);
			} else {
				TableDB.getInstance().removeTags(request.getAdIds(), tags);
			}
		}
	}	
	
	@DELETE
	@Path("resetAllTags")
	@Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
	public void resetAllTags() {
		TableDB.getInstance().resetAllTags();
	}
}
