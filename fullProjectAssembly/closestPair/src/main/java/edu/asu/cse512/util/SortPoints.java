package edu.asu.cse512.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import edu.asu.cse512.math.LineSegment;
import edu.asu.cse512.math.Point;

public class SortPoints {

	/**
	 * Sort a given set of points based on x and y coordinates
	 * 
	 * @param points
	 *            List of points
	 * @return Sorted list of points
	 */
	public static ArrayList<Point> sortPoints(List<Point> points) {
		Collections.sort(points, new Comparator<Point>() {
			public int compare(Point o1, Point o2) {
				return (o1.getX() > o2.getX() ? 1 : (o1.getX() == o2.getX() ? (o1.getY() > o2.getY() ? 1 : (o1.getY() == o2.getY() ? 0 : -1)) : -1));
			}
		});
		return (ArrayList<Point>) points;
	}

	/**
	 * Sort line segments based on distance
	 * 
	 * @param segments
	 *            List of line segments
	 * @return Sorted list of line segments
	 */
	public static ArrayList<LineSegment> sortLineSegment(List<LineSegment> segments) {
		Collections.sort(segments, new Comparator<LineSegment>() {
			public int compare(LineSegment o1, LineSegment o2) {
				return (o1.distance() > o2.distance() ? 1 : (o1.distance() == o2.distance() ? 0 : -1));
			}
		});
		return (ArrayList<LineSegment>) segments;
	}
}
