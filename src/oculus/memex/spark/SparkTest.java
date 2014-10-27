package oculus.memex.spark;

/* SimpleApp.java */
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

public class SparkTest {
	public static void main(String[] args) {
		String adFile = "/user/ehall/ht/ads/ads.csv"; 
		SparkConf conf = new SparkConf().setAppName("Simple Application");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> logData = sc.textFile(adFile).cache();
		
		long numAs = logData.filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = 1L;
			public Boolean call(String s) { return s.contains("jersey"); }
		}).count();
		
		long numBs = logData.filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = 1L;
			public Boolean call(String s) { return s.contains("toronto"); }
		}).count();
		
		System.out.println("Lines with jersey: " + numAs + ", lines with toronto: " + numBs);
	}
}