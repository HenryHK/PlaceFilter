package task3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import task3.TextIntPair;

// input PlaceName NumOfPhotos tags

public class PlaceNumTagMapper extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context) 
        throws IOException, InterruptedException{
        String[] fields = value.toString().split("\t");
        context.write(new Text(fields[1]), new Text(fields[0] + "\t" + fields[2]));
    }

}