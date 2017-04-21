package analysisTask1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import jdk.nashorn.internal.runtime.Context;

public class KeyChangeReducer extends Reducer<IntWritable, Text, Text, IntWritable>{

    public void reduce(IntWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException{
            for(Text text: values){
                context.write(text, key);
            }
    }

}