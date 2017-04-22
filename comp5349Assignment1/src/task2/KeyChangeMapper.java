package task2;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

// input LongWritable PlaceName \t frequency
// output frequency \t PlaceName

public class KeyChangeMapper extends Mapper<LongWritable, Text, Text, Text>{

    public void map(LongWritable key, Text value, Context context) 
        throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            context.write(new Text(fields[1]), new Text(fields[0]));
        }

}