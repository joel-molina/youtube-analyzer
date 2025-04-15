//main file.
//configures and starts MapReduce job.

package com.mycompany.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DegreeDistribution 
{
    public static void main(String[] args) throws Exception 
    {
        //create a new configuration and a MapReduce job based on this configuration.
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "degree distribution");

        //Explicitly state java scripts (their .class files) to the job.
        job.setJarByClass(DegreeDistribution.class);
        job.setMapperClass(DegreeMapper.class);
        job.setReducerClass(DegreeReducer.class);

        //set data types for the key-value pair emitted by the mapper class.
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DegreeWritable.class);

        //set data types for the key-value pair emitted by the reducer class.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //set input path to be defined in arg 0 and output path in arg 1.
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //exit job if done.
        boolean done = job.waitForCompletion(true);
        if(done)
        {
            System.exit(0);
        }
        else
        {
            System.exit(1);
        }
    }
}