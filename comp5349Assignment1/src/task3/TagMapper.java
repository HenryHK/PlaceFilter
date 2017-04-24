package task3;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

// inputfile PlaceName \t frequency \t tags
// output (tag, PlaceName) 1

public class TagMapper extends Mapper<LongWritable, Text, Text, Text>{

    public void map(LongWritable key, Text value, Context context) 
        throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            context.write(new Text(fields[1]), new Text(fields[0] + "\t" + fields[2]));
        }

}