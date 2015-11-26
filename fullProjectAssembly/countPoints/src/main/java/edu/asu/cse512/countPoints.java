package edu.asu.cse512;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.google.common.collect.Iterables;

import edu.asu.cse512.math.Point;
import edu.asu.cse512.math.Polygon;
import edu.asu.cse512.util.Constants;

/**
 * Hello world!
 *
 */
public class countPoints 
{
    public static void main( String[] args )
    {
    	String testData = args[0];
		String qRect = args[1];
		String outputFile = args[2];
		countPointsInside(testData,qRect,outputFile);
    }
    
    private static void countPointsInside(String testData, String qRect, String outputFile) {

		SparkConf conf = new SparkConf().setAppName(Constants.APP_NAME)
				.setMaster(Constants.MASTER);
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Contents of test data file
		JavaRDD<String> inputPoints = sc.textFile(testData).cache();

		// Contents of query polygon file
		JavaRDD<String> inputTestPolygons = sc.textFile(qRect).cache();

		// Map entries of query rectangle into RDD of Polygon and filter out null values
		JavaRDD<Polygon> queryRect = inputTestPolygons.map(
				new Function<String, Polygon>() {
					public Polygon call(String s) throws Exception {
						String[] parts = s.split(",");
						if (!parts[0].matches("-?\\d+(\\.\\d+)?")) {
							return null;
						}
						long id = Long.parseLong(parts[0]);
						Point p1 = new Point(Double.parseDouble(parts[1]),
								Double.parseDouble(parts[2]));
						Point p2 = new Point(Double.parseDouble(parts[3]),
								Double.parseDouble(parts[4]));
						Polygon p = new Polygon(p1, p2, id);
						return p;
					}
				}).filter(new Function<Polygon, Boolean>() {

			public Boolean call(Polygon v1) throws Exception {
				if (v1 == null)
					return false;
				else
					return true;
			}
		});
		
		// Convert input file to RDD of points and filter out null values
		JavaRDD<Point> points = inputPoints.map(new Function<String, Point>() {
					public Point call(String s) throws Exception {
						String[] points = s.split(",");
						if (!points[0].matches("-?\\d+(\\.\\d+)?")) {
							return null;
						}
						long id = Long.parseLong(points[0]);
						double x1 = Double.parseDouble(points[1]);
						double y1 = Double.parseDouble(points[2]);
						return new Point(x1, y1, id);
					}
				}).filter(new Function<Point, Boolean>() {
					public Boolean call(Point v1) throws Exception {
						if (v1 == null)
							return false;
						else
							return true;
					}
				});

		// Find points that join with each query polygons
		JavaPairRDD<Long, Long> joins = queryRect.cartesian(points).mapToPair(new PairFunction<Tuple2<Polygon, Point>, Long, Long>() {
					public Tuple2<Long, Long> call(Tuple2<Polygon, Point> arg0) throws Exception {
						Polygon set = arg0._1();
						Point point = arg0._2();
						if (!set.contains(point))
							return null;
						return new Tuple2<Long, Long>(set.getId(), point.getId());
					}
				}).filter(new Function<Tuple2<Long, Long>, Boolean>() {
					public Boolean call(Tuple2<Long, Long> arg0) throws Exception {
						if (arg0 == null)
							return false;
						else
							return true;
					}
				});

		
		// Group and sort by the query rectangle
		JavaPairRDD<Long, Iterable<Long>> result = joins.groupByKey().sortByKey();
				// Create String RDD to match output format
				JavaRDD<String> outputFormat = result.map(new Function<Tuple2<Long, Iterable<Long>>, String>() {
					public String call(Tuple2<Long, Iterable<Long>> v1) throws Exception {
						String output = v1._1 + " , "+Iterables.size(v1._2);
						return output;
					}
				});
				// Save point join results into a text file
				outputFormat.saveAsTextFile(outputFile);
	}

}
