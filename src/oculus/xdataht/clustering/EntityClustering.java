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
package oculus.xdataht.clustering;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import oculus.xdataht.data.DataRow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oculusinfo.ml.DataSet;
import com.oculusinfo.ml.Instance;
import com.oculusinfo.ml.centroid.Centroid;
import com.oculusinfo.ml.distance.DistanceFunction;
import com.oculusinfo.ml.feature.spatial.centroid.GeoSpatialCentroid;
import com.oculusinfo.ml.feature.spatial.distance.HaversineDistance;
import com.oculusinfo.ml.feature.string.StringFeature;
import com.oculusinfo.ml.unsupervised.cluster.AbstractClusterer;
import com.oculusinfo.ml.unsupervised.cluster.Cluster;
import com.oculusinfo.ml.unsupervised.cluster.ClusterResult;
import com.oculusinfo.ml.unsupervised.cluster.Clusterer;
import com.oculusinfo.ml.unsupervised.cluster.InMemoryClusterResult;
import com.oculusinfo.ml.unsupervised.cluster.dpmeans.DPMeans;
import com.oculusinfo.ml.unsupervised.cluster.kmeans.KMeans;
import com.oculusinfo.ml.validation.unsupervised.internal.Cohesion;
import com.oculusinfo.ml.validation.unsupervised.internal.Separation;


public class EntityClustering implements Serializable {
	private static final long serialVersionUID = -7287997469823771918L;
	private static final Logger log = LoggerFactory.getLogger("com.oculusinfo");
	
	private ArrayList<DataRow> _rows;
	private ArrayList<String> _clusterBy;
	private ArrayList<Double> _weights;
	private HashMap<String, DataSet> _datasetMap = new HashMap<String, DataSet>(); 
	
	private static HashSet<String> _notFuzzy = new HashSet<String>();
	static {
		_notFuzzy.add("phone");
		_notFuzzy.add("appearance");
		_notFuzzy.add("email");
		_notFuzzy.add("website");
		_notFuzzy.add("ethnicity");
		_notFuzzy.add("age");
		_notFuzzy.add("eye_color");
	}
	
	public EntityClustering(ArrayList<DataRow> rows, ArrayList<String> clusterBy, ArrayList<Double> weights) {
		_rows = rows;
		_clusterBy = clusterBy;
		_weights = normalizeWeights(weights);
	}
	
	private ArrayList<Double> normalizeWeights(ArrayList<Double> weights) {
		ArrayList<Double> normalized = new ArrayList<Double>();
		
		double total = 0;
		for (Double weight : weights) {
			total += weight;
		}
		
		for (Double weight : weights) {
			normalized.add(weight / total);
		}
		return normalized;
	}
	
	private double normalizeThreshold(ArrayList<Double> weights, double alpha) {
		double total = 0;
		for (Double weight : weights) {
			total += weight;
		}
		return alpha * total;
	}
	
	private boolean isValidField(String value) {
		return (value != null && !value.equalsIgnoreCase("null") && !value.equalsIgnoreCase("n/a") && !value.isEmpty());
	}
	
	private String normalizeString(String raw) {
		if (raw == null) {
			return "NULL";
		} else {
			return raw.trim().toLowerCase();
		}
	}
	
	private String normalizePhoneNumber(String raw) {
		
		if (raw == null) {
			return "NULL";
		}

		String cleaned = normalizeString(raw);
		StringBuilder phone = new StringBuilder();
		
		cleaned = cleaned.replace("one", "1");
		cleaned = cleaned.replace("two", "2");
		cleaned = cleaned.replace("three", "3");
		cleaned = cleaned.replace("four", "4");
		cleaned = cleaned.replace("five", "5");
		cleaned = cleaned.replace("six", "6");
		cleaned = cleaned.replace("seven", "7");
		cleaned = cleaned.replace("eight", "8");
		cleaned = cleaned.replace("nine", "9");
			
		for (int i=0; i < cleaned.length(); i++) {
			String c = raw.substring(i, i+1);
			
			if (c.matches("[0-9]")) {
				phone.append(c);
			}
		}
		if (phone.length() < 7) return "";  // not a valid phone number
		return phone.toString();
	}
	
	private String createAppearanceHash(DataRow fields) {
		// TODO what else do we want in an appearance hash?  Height?  Bust?
		String ethnicity = normalizeString(fields.get("ethnicity"));
		String eye = normalizeString(fields.get("eye_color"));
		String build = normalizeString(fields.get("build"));
		return ethnicity + eye + build;
	}
	
	private DataSet createDataSet() throws IOException {
		DataSet ds = new DataSet();

		for (int rowIdx = 0; rowIdx < _rows.size(); rowIdx++) {
			
			if (rowIdx % 5000 == 0) {
				log.info("\t" + rowIdx + " of " + _rows.size());
			}
			
			DataRow fields = _rows.get(rowIdx); 
		
			String id = fields.get("id");
			Instance inst = new Instance(id);

			StringFeature feature;
			for (String field:_clusterBy) {
				String val = "";
				if (field.equals("appearance")) {
					val = createAppearanceHash(fields);
				} else if (field.equals("phone")) {
					val = normalizePhoneNumber(fields.get("phone"));
				} else {
					val = normalizeString(fields.get(field));
				}
				if (isValidField(val)) {
					feature = new StringFeature(field);
					feature.setValue(val);
					inst.addFeature(feature);
				}
			}
			
			if (!inst.getAllFeatures().isEmpty()) {
				ds.add(inst);
			} 
		}

		return ds;
	}
	
	public void outputValidation(Clusterer clusterer, ClusterResult clusters) {
		double separation = Separation.validate(clusterer, clusters);
		double cohesion = Cohesion.validate(clusterer, clusters);
		
		System.out.println("Cohesion: " + cohesion);
		System.out.println("Separation: " + separation);
	}
	
	@SuppressWarnings("rawtypes")
	private Clusterer createClusterer(boolean course, boolean outputMsg) {
		AbstractClusterer clusterer = null;
		if (course) {
//			DPMeans dp = new DPMeans(true, 5, false);
//			dp.setThreshold( normalizeThreshold(this._weights, 0.7) );
//			clusterer = dp;
			clusterer = new KMeans(20, 5, false);
		}
		else {
			
			DPMeans dp = new DPMeans(5, true);
			dp.setThreshold(normalizeThreshold(this._weights, 0.5));
			clusterer = dp;
			
//			double threshold = 0.5; //normalizeThreshold(this._weights);
//			clusterer = new ThresholdClusterer(false, true);
//			((ThresholdClusterer)clusterer).setThreshold( threshold ); // TODO this should be exposed as a setting and not hard coded
//			log.info("Cluster thresohold: {}", threshold);
		}
		
		clusterer.setLogger(log);
		
		if (outputMsg) log.info("Cluster features: ");
		
		for (int i = 0; i < _clusterBy.size(); i++) {
			String featureKey = _clusterBy.get(i);
			double featureWeight = _weights.get(i);
			
			DistanceFunction distanceFunc = null;
			Class<? extends Centroid> centroidClass = null;
			
			if (featureKey.equalsIgnoreCase("geo")) {
				distanceFunc = new HaversineDistance(featureWeight);
				centroidClass = GeoSpatialCentroid.class;
			} else if (!_notFuzzy.contains(featureKey)) {
				distanceFunc = new com.oculusinfo.ml.feature.string.distance.EditDistance(featureWeight);
				centroidClass = com.oculusinfo.ml.feature.string.centroid.StringMedianCentroid.class;
			} else {
				featureWeight = 0.5 * featureWeight;
				distanceFunc = new com.oculusinfo.ml.feature.string.distance.ExactTokenMatchDistance(featureWeight);
				centroidClass = com.oculusinfo.ml.feature.string.centroid.StringMedianCentroid.class;
			}
			
			if (outputMsg) log.info("* " + featureKey + " weight = " + featureWeight);
			clusterer.registerFeatureType(featureKey, centroidClass, distanceFunc);
		}
		
		return clusterer;
	}
	
	public static void dumpToFile(ClusterResult result) {
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter("output.txt"));
			
			for (Cluster c : result) {
				writer.write(c.toString(true) + "\n");
			}
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public ClusterResult cluster(String datasetName) throws InterruptedException {
		ClusterResult clusters = null;
		
		try {
			log.info("Loading dataset");
			DataSet ds = _datasetMap.get(datasetName);
			if (ds == null) {
				ds = createDataSet();
				_datasetMap.put(datasetName, ds);
			}
			log.info("Dataset contains " + ds.size() + " instances");
			
			long start = System.currentTimeMillis();
			
			log.info("Creating clusterer");
			Clusterer courseClusterer = createClusterer(true, false);
			Clusterer entityClusterer = createClusterer(false, true);
			
			log.info("Clustering entities");
			ClusterResult courseClusters = courseClusterer.doCluster(ds);
			log.info("Generated {} course clusters", courseClusters.size());
			List<Cluster> results = new LinkedList<Cluster>();
						
			log.info("sub-clustering course clusters");
			// sub-cluster the course clusters
			for (Cluster c : courseClusters) {
				DataSet d = new DataSet();
				d.addAll(c.getMembers());
				ClusterResult entityClusters = entityClusterer.doCluster(d);
				
				for (Cluster e : entityClusters) {
					results.add(e);				
				}
			}
			
			// construct the entity cluster result set
			clusters = new InMemoryClusterResult(results);
			
			log.info("Generated " + clusters.size() + " entity clusters");
			
			log.info("Total clustering time (s): {}", (System.currentTimeMillis()-start)/1000.0);
			
			double ave = 0;
			for (Cluster c : clusters) {
				ave += c.size();
			}
			log.info("Average cluster size: " + (ave / clusters.size()));
			
//			// Test code to dump cluster scores
//			Map<String, Double> scores = scoreClusters(entityClusterer, clusters);
//			for (Cluster c : clusters) {
//				System.out.println("Cluster " + c.getId() + ": score = " + scores.get(c.getId()));
//				Map<String, Double> iscores = scoreInstanceMembers(entityClusterer, c);
//				for (String id : iscores.keySet()) {
//					System.out.println("Instance " + ": score = " + iscores.get(id));
//				}
////			System.out.println(c.toString(true));  // uncomment to dump cluster contents to console
//				System.out.println("--------------");
//			}
			
			// Test code to dump clustering validation
//			log.info("Validating Clusters");
//			outputValidation(clusterer, clusters);
			
//			dumpToFile(clusters);
			
			log.info("Done");
		} catch (IOException e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
		return clusters;
	}
	
	/***
	 * Computes a confidence score of an instance belonging to a cluster   
	 * 
	 * @param clusterer used to cluster instances
	 * @param cluster the instance belongs to
	 * @param instance to score
	 * @return confidence score in [0,1] where 0 = no confidence and 1 = complete confidence
	 */
	public double scoreInstanceMember(Clusterer clusterer, Cluster cluster, Instance instance) {
		double score = 0;
		
		for (Instance otherInst : cluster.getMembers()) {
			score += clusterer.distance(instance, otherInst);
		}
		return 1.0 - (score / cluster.size());
	}
	
	/***
	 * Computes a confidence score of all instances belonging to a cluster   
	 * 
	 * @param clusterer used to cluster instances
	 * @param cluster to score
	 * @return A map of instance id's and confidence score in [0,1] where 0 = no confidence and 1 = complete confidence
	 */
	public Map<String, Double> scoreInstanceMembers(Clusterer clusterer, Cluster cluster) {
		Map<String, Double> scores = new HashMap<String, Double>();
		
		for (Instance instance : cluster.getMembers()) {
			scores.put(instance.getId(), scoreInstanceMember(clusterer, cluster, instance));
		}
		return scores;
	}
	
	/***
	 * Computes a confidence score of a resulting cluster and the likelihood of the instances being grouped together
	 * 
	 * @param clusterer used to cluster instances
	 * @param cluster to score
	 * @return confidence score in [0,1] where 0 = no confidence and 1 = complete confidence
	 */
	public double scoreCluster(Clusterer clusterer, Cluster cluster) {
		return 1.0 - Cohesion.cohesion(clusterer, cluster);
	}

	/***
	 * Computes a confidence score of all clusters    
	 * 
	 * @param clusterer used to cluster instances
	 * @param clusters to score
	 * @return A map of cluster id's and confidence score in [0,1] where 0 = no confidence and 1 = complete confidence
	 */
	public Map<String, Double> scoreClusters(Clusterer clusterer, ClusterResult clusters) {
		Map<String, Double> scores = new HashMap<String, Double>();
		
		for (Cluster cluster : clusters) {
			scores.put(cluster.getId(), scoreCluster(clusterer, cluster));
		}
		return scores;
	}
}
