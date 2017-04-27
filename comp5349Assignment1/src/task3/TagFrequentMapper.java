package task3;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import task3.TextTextPair;

//input PlaceName \t frequency \t tags (top50)
//output (PlaceName tag) 1

//TODO add combiner to this
public class TagFrequentMapper extends Mapper<LongWritable, Text, TextTextPair, Text>{

    TextTextPair k = new TextTextPair();
            Text nameText = new Text();
            Text tagText = new Text();
            Text v = new Text("1");
            String[] fields;
            String place_name;
            String[] tags;

    public void map(LongWritable key, Text text, Context context)
        throws IOException, InterruptedException{

            fields = text.toString().split("\t");
            place_name = fields[0];
            tags = fields[2].split(" ");
            
            for (String tag : tags){
                nameText.set(place_name);
                tagText.set(tag);
                k.setKey(nameText);
                k.setOrder(tagText);
                context.write(k, v);
                tag = null;
            }

            fields = null;
            tags = null;
            place_name = null;
            System.gc();
    }

}