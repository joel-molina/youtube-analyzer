package com.mycompany.hadoop;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DegreeWritable implements Writable 
{
    private int inDegree;
    private int outDegree;

    //no-arg constructor
    public DegreeWritable() 
    {
        this.inDegree = 0;
        this.outDegree = 0;
    }

    //parameterized constructor
    public DegreeWritable(int inDegree, int outDegree) 
    {
        this.inDegree = inDegree;
        this.outDegree = outDegree;
    }

    //getters
    public int getInDegree() 
    {
        return inDegree;
    }
    public int getOutDegree() 
    {
        return outDegree;
    }

    //setters
    public void setInDegree(int inDegree) 
    {
        this.inDegree = inDegree;
    }
    public void setOutDegree(int outDegree) 
    {
        this.outDegree = outDegree;
    }

    //write to datastream
    @Override
    public void write(DataOutput out) throws IOException 
    {
        out.writeInt(inDegree);
        out.writeInt(outDegree);
    }

    //read from datastream
    @Override
    public void readFields(DataInput in) throws IOException 
    {
        inDegree = in.readInt();
        outDegree = in.readInt();
    }
}