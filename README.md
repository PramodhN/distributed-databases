# CSE 512 - Course Project

Repository for CSE 512 - Distributed and Parellel Databases - course project. 

Project Description
===================
The aim of this project is to perform distributed geo-spatial operations using MapReduce. The operations performed are:
* Geometric Union
* Convex Hull
* Closest and Farthest Pair
* Spatial Range
* Spatial Join

Dataset
-------
The dataset contains set of points and polygons. Each point is latitude and longitude. Each polygon consists of two points, the top left(x1,y1) and bottom right(x2,y2) with which we can construct a rectangle.

Technologies Used
=================
- Java (Maven)
  * org.apache.spark
  * math.geom2d
- Hadoop Distributed File System
- Apache Spark
 
Running the program
===================
Start spark, set up the worker nodes and run the following line in *bin/* folder
```
./spark-submit  --class edu.asu.cse512.ClosestPair \
                --master spark://SPARK-MASTER-URL \
                closestPair.jar \
                hdfs://INPUT-FILE-LOCATION \
                hdfs://OUTPUT-FOLDER
```

Contributors
============
- [Pramodh](https://github.com/PramodhN/)
- [Ankita](https://github.com/apchandak24)
- [Rajat](https://github.com/rajataggarwal91)
- [Jasdeep](https://github.com/jasdeepbhalla)
- [Sravya](https://github.com/sravyaguturu)
- [Nicolette](https://github.com/NicoletteFurtado)
