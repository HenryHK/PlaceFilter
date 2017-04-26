package task3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.sun.org.apache.xalan.internal.xsltc.dom.SAXImpl.NamespaceWildcardIterator;

import task3.TextTextPair;

// input (PlaceName, tag) 1
// output (PlaceName, total) tag

public class TopTagReducer extends Reducer<TextIntPair, Text, Text, Text>{

    public void reduce(TextIntPair key, Iterable<Text> values, 
			Context context
	) throws IOException, InterruptedException {
        String result = "";
        String place_name = key.getKey().toString();
        int count = 0;
        for(Text text : values){
            String[] info = text.toString().split("\t");
            String tag = info[0];
            String freq = info[1];
            result += "("+tag + ": " + freq + ") ";
            if(++count>=10)
                break;
        }
        context.write(new Text(place_name), new Text(result));

        // String result = "";
        // String place_name = key.getKey().toString();
        // int total = key.getOrder().get();
        // for (Text value : values){
        //     String tag = value.toString();
        //     result += tag + "\t" +Integer.toString(total)+ "\t";
        // }
        // context.write(new Text(place_name), new Text(result));
        
    }
}