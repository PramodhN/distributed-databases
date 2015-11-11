package edu.asu.cse512;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;

import edu.asu.cse512.math.LineSegment;
import edu.asu.cse512.math.Point;
import edu.asu.cse512.math.Polygon;
import edu.asu.cse512.util.Constants;

/**
 * Find farthest pair of points among a set of given points
 * 
 * @author pramodh
 *
 */
public class FarthestPair {
	/**
	 * @param args
	 *            args[0] = input file, args[1] = output file
	 */
	@SuppressWarnings("serial")
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName(Constants.APP_NAME);
		@SuppressWarnings("resource")
		JavaSparkContext spark = new JavaSparkContext(conf);

		JavaRDD<String> input = spark.textFile(args[0]);

		// Map input file to polygons containing a single points and filter the points with null values.
		JavaRDD<Polygon> polygon = input.map(new Function<String, Polygon>() {
			public Polygon call(String s) {
				String[] points = s.split(",");
				if (!points[0].matches("-?\\d+(\\.\\d+)?")) {
					return null;
				}
				return new Polygon(Double.parseDouble(points[0]), Double.parseDouble(points[1]));
			}
		}).filter(new Function<Polygon, Boolean>() {
			public Boolean call(Polygon v1) throws Exception {
				if (v1 == null)
					return false;
				else
					return true;
			}
		});

		// Compute the convex hull of all the points.
		Polygon convexHull = polygon.reduce(new Function2<Polygon, Polygon, Polygon>() {
			public Polygon call(Polygon arg0, Polygon arg1) throws Exception {
				return arg0.convexHull(arg1);
			}
		});

		// Create an RDD of just the vertices on the convex hull
		JavaRDD<Point> convexVertices = spark.parallelize(convexHull.getPoints());

		// Create a broadcast list of all points
		final Broadcast<List<Point>> convexBroadcastPoints = spark.broadcast(convexVertices.take((int) convexVertices.count()));

		// Create RDD of line segments of each point with its closest point
		JavaRDD<LineSegment> maxDistPair = convexVertices.map(new Function<Point, LineSegment>() {
			public LineSegment call(Point point) {
				double maxDist = Double.MIN_VALUE;
				Point otherPoint = null;
				for (int i = 0; i < convexBroadcastPoints.getValue().size(); i++) {
					Point p = convexBroadcastPoints.getValue().get(i);
					if (!p.equals(point)) {
						double distance = new LineSegment(p, point).distance();
						if (distance > maxDist) {
							maxDist = distance;
							otherPoint = p;
						}
					}
				}
				LineSegment lineSeg = new LineSegment(point, otherPoint);
				return lineSeg;
			}
		});

		// Sort all the line segments in descending order based on the distance between them
		JavaRDD<LineSegment> segments = maxDistPair.sortBy(new Function<LineSegment, Double>() {
			public Double call(LineSegment v) throws Exception {
				return v.distance();
			}
		}, false, 1);

		// Save first result from the sorted RDD of line segments into a list.
		ArrayList<LineSegment> output = new ArrayList<LineSegment>();
		LineSegment farthestPair = segments.first();
		output.add(farthestPair);

		// Save the minimum distance points to a text file
		spark.parallelize(output).repartition(1).saveAsTextFile(args[1]);
	}

}
