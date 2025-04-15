//Reducer class to to aggregate key values in the file.
//Sum up values for every key to get frequency of each category.
package com.mycompany.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

//takes in text keys (videoID), list of degreewritable values, output text
public class CategoryReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
{
    private IntWritable result = new IntWritable();


    //runs per category
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
    {
        int sum = 0;

        //iterate through the intwritable key-value pairs and aggregate.
        for (IntWritable val : values) 
        {
            sum += val.get();
        }

        //emit frequency per category
        result.set(sum);
        context.write(key, result);
    }
}