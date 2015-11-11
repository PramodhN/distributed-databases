package edu.asu.cse512;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import edu.asu.cse512.math.LineSegment;
import edu.asu.cse512.math.Point;
import edu.asu.cse512.util.Constants;

/**
 * Find closest pair between set of points
 * 
 * @author pramodh
 *
 */
public class ClosestPair {

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

		// Map input file to points and filter the polygons with null values
		JavaRDD<Point> points = input.map(new Function<String, Point>() {
			public Point call(String s) {
				String[] points = s.split(",");
				if (!points[0].matches("-?\\d+(\\.\\d+)?")) {
					return null;
				}
				return new Point(Double.parseDouble(points[0]), Double.parseDouble(points[1]));
			}
		}).filter(new Function<Point, Boolean>() {
			public Boolean call(Point v1) throws Exception {
				if (v1 == null)
					return false;
				else
					return true;
			}
		});

		// Create a broadcast list of all points
		final Broadcast<List<Point>> allPoints = spark.broadcast(points.take((int) points.count()));

		// Create RDD of line segments of each point with its closest point
		JavaRDD<LineSegment> minDistPair = points.map(new Function<Point, LineSegment>() {
			public LineSegment call(Point point) {
				double minDist = Double.MAX_VALUE;
				Point otherPoint = null;
				for (int i = 0; i < allPoints.getValue().size(); i++) {
					Point p = allPoints.getValue().get(i);
					if (!p.equals(point)) {
						double distance = new LineSegment(p, point).distance();
						if (distance < minDist && minDist != 0.0) {
							minDist = distance;
							otherPoint = p;
						}
					}
				}
				LineSegment lineSeg = new LineSegment(point, otherPoint);
				return lineSeg;
			}
		});

		// Sort all the line segments in ascending order based on the distance between them
		JavaRDD<LineSegment> segments = minDistPair.sortBy(new Function<LineSegment, Double>() {
			public Double call(LineSegment v) throws Exception {
				return v.distance();
			}
		}, true, 1);

		// Save first result from the sorted RDD of line segments into a list.
		ArrayList<LineSegment> output = new ArrayList<LineSegment>();
		LineSegment closestPair = segments.first();
		output.add(closestPair);

		// Save the minimum distance points to a text file
		spark.parallelize(output).repartition(1).saveAsTextFile(args[1]);
	}

}
