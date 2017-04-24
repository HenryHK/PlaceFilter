package task3;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import task3.TextTextPair;

//input placeName total tag
//output (placename total) tag

public class TopTagMapper extends Mapper<LongWritable, Text, TextIntPair, Text>{
    public void map(LongWritable key, Text text, Context context)
        throws IOException, InterruptedException{
            String[] t = text.toString().split("\t");
            if(t.length>2)
                context.write(new TextIntPair(t[0], Integer.parseInt(t[1])), new Text(t[2]));
    }
}
