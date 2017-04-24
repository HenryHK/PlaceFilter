package task3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import task3.TextTextPair;

import org.apache.hadoop.io.*;

public class TopTagSortComparator extends WritableComparator{
    
    protected TopTagSortComparator(){
        super(TextIntPair.class, true);
    }

    public int compare(WritableComparable w1, WritableComparable w2){
        TextIntPair t1 = (TextIntPair)w1;
        TextIntPair t2 = (TextIntPair)w2;

        return -t1.compareTo(t2);
    }

}