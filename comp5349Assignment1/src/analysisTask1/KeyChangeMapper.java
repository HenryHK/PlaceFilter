package analysisTask1;

import java.io.IOException;

import javax.swing.plaf.metal.MetalIconFactory.PaletteCloseIcon;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Text;

public class KeyChangeMapper extends Mapper<Text, Text, IntWritable, Text>{

    private IntWritable freq = new IntWritable();

    public void map(Text key, Text value, Context context) 
        throws IOException, InterruptedException {
            freq.set(Integer.parseInt(value.toString()));
            context.write(freq, key);
        }

}