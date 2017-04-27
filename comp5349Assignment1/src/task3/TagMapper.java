package task3;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

// inputfile PlaceName \t frequency \t tags
// output (tag, PlaceName) 1

public class TagMapper extends Mapper<LongWritable, Text, Text, Text>{

    

    public void map(LongWritable key, Text value, Context context) 
        throws IOException, InterruptedException {
            Text k = new Text();
            Text v = new Text();
            String[] fields;
            fields = value.toString().split("\t");
            if(fields.length > 2){
                k.set(fields[1]);
                v.set(fields[0] + "\t" + fields[2]);
                context.write(k,v);
            }
        }

}