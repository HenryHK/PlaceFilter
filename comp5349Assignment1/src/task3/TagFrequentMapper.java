package task3;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import task3.TextTextPair;

//input PlaceName \t frequency \t tags (top50)
//output (PlaceName tag) 1
public class TagFrequentMapper extends Mapper<LongWritable, Text, TextTextPair, Text>{

    public void map(LongWritable key, Text text, Context context)
        throws IOException, InterruptedException{
            String[] fields = text.toString().split("\t");
            String place_name = fields[0];
            String[] tags = fields[2].split(" ");
            for (String tag : tags){
                context.write(new TextTextPair(place_name,tag), new Text("1"));
            }
    }

}