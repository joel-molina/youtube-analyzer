//Mapper class to read in the file.
//Maps each category to a key with a value of 1.
package com.mycompany.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//extends Mapper<input key, input value, output key, output value>
public class CategoryMapper extends Mapper<Object, Text, Text, IntWritable> 
{
    //oject to hold and emit videoIDs as an output key.
    private Text category = new Text();
    private final static IntWritable one = new IntWritable(1);

    //override map method (value is the line of text context is to emit key-value pairs)
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    {
        //skip header
        String line = value.toString();
        if(line.contains("video_id"))
        {
            return;
        }

        //TSV so tab delimiter.
        String[] fields = value.toString().split("\t");

        //read in "category" which is in 4th column.
        category.set(fields[3]);
        context.write(category, one);
    }
}
