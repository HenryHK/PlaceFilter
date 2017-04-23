package task3;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

//input PlaceName \t frequency \t tags (top50)
//output PlaceName \t tag
public class TagFrequentMapper extends Mapper<LongWritable, Text, Text, Text>{

}