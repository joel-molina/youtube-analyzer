//main file.
//configures and starts MapReduce job.

package com.mycompany.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;

public class CategoryDistribution 
{
    public static void main(String[] args) throws Exception 
    {
        //create a new configuration and a MapReduce job based on this configuration.
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "category distribution");

        //Explicitly state java scripts (their .class files) to the job.
        job.setJarByClass(CategoryDistribution.class);
        job.setMapperClass(CategoryMapper.class);
        job.setReducerClass(CategoryReducer.class);

        //set data types for the key-value pair emitted by the mapper class.
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //set data types for the key-value pair emitted by the reducer class.
        //redundant since mapreduce automatically takes mapper types and they are the same in the reducer.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

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