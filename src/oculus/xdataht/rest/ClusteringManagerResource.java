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

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import oculus.xdataht.clustering.ClusteringManager;
import oculus.xdataht.data.TableDB;
import oculus.xdataht.model.ClusterRequest;
import oculus.xdataht.model.ClusteringJobInfo;
import oculus.xdataht.model.ClusteringJobs;

@Path("/clusterManager")
public class ClusteringManagerResource  {
	@Context
	UriInfo _uri;
	
	@GET
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public ClusteringJobs getActiveJobs() {
		ClusteringJobs jobs = new ClusteringJobs(ClusteringManager.getInstance().getActiveJobs(), ClusteringManager.getInstance().getCompletedJobs());
		return jobs;
	}
	
	@GET
	@Path("{handle}")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public ClusteringJobInfo getJobInfo(@PathParam("handle") Integer handle) {
		return ClusteringManager.getInstance().getJobInfo(handle);
	}
	
	@POST
	@Path("new")
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	@Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
	public ClusteringJobInfo newJob(ClusterRequest request) throws WebApplicationException{
		ClusteringJobInfo job = ClusteringManager.getInstance().createJob(request);
		if (job == null) {
			throw new WebApplicationException();
		}
		return job;
	}
	
	@DELETE
	@Path("kill/{handle}")
	public void killJob(@PathParam("handle") Integer handle) {
		ClusteringManager.getInstance().requestKill(handle);
	}
	
	@DELETE
	@Path("removeCompleted/{handle}")
	public void removeCompleted(@PathParam("handle") Integer handle) {
		ClusteringManager.getInstance().removeRecentlyCompletedJob(handle);
	}
	
	@DELETE
	@Path("delete/{datasetname}/{clustersetName}")
	public String handleDelete(@PathParam("datasetName") String datasetName, @PathParam("clustersetName") String clustersetName) {
		TableDB.getInstance().deleteClusterset(datasetName, clustersetName);
		return "{\"done\":true}";
	}
}
