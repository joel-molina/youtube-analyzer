//Reducer class to to aggregate key values in the file.
//Sum up values for every key to get degree of each video.
package com.mycompany.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

//takes in text keys (videoID), list of degreewritable values, output text
public class DegreeReducer extends Reducer<Text, DegreeWritable, Text, Text> 
{
    //track global statistics 
    private int totalDegree = 0;
    private int maxDegree = Integer.MIN_VALUE;
    private int minDegree = Integer.MAX_VALUE;

    //track in-degree statistics
    private int totalInDegree = 0;
    private int maxInDegree = Integer.MIN_VALUE;
    private int minInDegree = Integer.MAX_VALUE;

    //track out-degree statistics
    private int totalOutDegree = 0;    
    private int maxOutDegree = Integer.MIN_VALUE;
    private int minOutDegree = Integer.MAX_VALUE;

    private int nodeCount = 0;

    //runs per videoID (key)
    public void reduce(Text key, Iterable<DegreeWritable> values, Context context) throws IOException, InterruptedException 
    {
        int inDegree = 0;
        int outDegree = 0;

        //iterate through the degreewritable key-value pairs and aggregate.
        for (DegreeWritable val : values) 
        {
            inDegree += val.getInDegree();
            outDegree += val.getOutDegree();
        }

        int degree = inDegree + outDegree;

        //update global statistics
        totalDegree += degree;
        maxDegree = Math.max(maxDegree, degree);
        minDegree = Math.min(minDegree, degree);

        //update in-degree statistics
        totalInDegree += inDegree;
        maxInDegree = Math.max(maxInDegree, inDegree);
        minInDegree = Math.min(minInDegree, inDegree);

        //update out-degree statistics
        totalOutDegree += outDegree;
        maxOutDegree = Math.max(maxOutDegree, outDegree);
        minOutDegree = Math.min(minOutDegree, outDegree);


        nodeCount++;

        //emit final output for the video (videoID, in-degree, out-degree)
        context.write(key, new Text("In-Degree: " + inDegree + ", Out-Degree: " + outDegree));
    }

    //Once all reduce calls are done, print summary statistics at the end of the file/
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException 
    {
        double averageDegree = (double) totalDegree / nodeCount;
        double averageInDegree = (double) totalInDegree / nodeCount;
        double averageOutDegree = (double) totalOutDegree / nodeCount;

        context.write(new Text("Average Degree"), new Text(String.valueOf(averageDegree)));
        context.write(new Text("Max Degree"), new Text(String.valueOf(maxDegree)));
        context.write(new Text("Min Degree"), new Text(String.valueOf(minDegree)));

        context.write(new Text("Average In-Degree"), new Text(String.valueOf(averageInDegree)));
        context.write(new Text("Max In-Degree"), new Text(String.valueOf(maxInDegree)));
        context.write(new Text("Min In-Degree"), new Text(String.valueOf(minInDegree)));

        context.write(new Text("Average Out-Degree"), new Text(String.valueOf(averageOutDegree)));
        context.write(new Text("Max Out-Degree"), new Text(String.valueOf(maxOutDegree)));
        context.write(new Text("Min Out-Degree"), new Text(String.valueOf(minOutDegree)));
    }
}