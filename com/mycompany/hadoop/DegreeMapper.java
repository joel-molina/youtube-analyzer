//Mapper class to read in the file.
//Maps each related videoID to a key with a value of 1.
package com.mycompany.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//extends Mapper<input key, input value, output key, output value>
public class DegreeMapper extends Mapper<Object, Text, Text, DegreeWritable> 
{
    //oject to hold and emit videoIDs as an output key.
    private Text videoID = new Text();

    //override map method (value is the line of text context is to emit key-value pairs)
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    {
        //TSV so tab delimiter.
        String[] fields = value.toString().split("\t");

        //if >10 fields means there ARE related videos.
        if (fields.length > 10) 
        {
            //emit out degree for main video.
            videoID.set(fields[0]); //set text object to current video ID
            DegreeWritable outDegreeWritable = new DegreeWritable(0, fields.length - 9);
            context.write(videoID, outDegreeWritable);

            //emit in-degree for every related video.
            for (int i = 9; i < fields.length; i++) 
            {
                if (!fields[i].isEmpty()) 
                {
                    videoID.set(fields[i]);
                    DegreeWritable inDegreeWritable = new DegreeWritable(1, 0);
                    context.write(videoID, inDegreeWritable);
                }
            }
        }
    }
}

