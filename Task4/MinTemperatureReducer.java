/**
 * @Author: Yu Gu
 * Andrew ID: ygu3
 */

package edu.cmu.andrew.mm6;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer; import org.apache.hadoop.mapred.Reporter;

public class MinTemperatureReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * Function to perform Hadoop's reduce operation. It gets the key:value pairs from the
     * mapper function, reduces the output from mapper to find the minimum temperature for
     * each year in the input file
     * @param key Text as input key to the reduce function
     * @param values Iterable<Text> as values to the reduce function
     * @param output Context to form output of reduce function
     * @param reporter Reporter for the reducer program
     * @throws IOException Exception while performing IO operations
     */
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text,
            IntWritable> output, Reporter reporter) throws IOException {

        // Initialize the minValue to the maximum possible value of an integer
        int minValue = Integer.MAX_VALUE;

        // from the list of values, find the minimum
        while (values.hasNext()) {
            minValue = Math.min(minValue, values.next().get());
        }

        // emit (key = year, value = minTemp = min for year)
        output.collect(key, new IntWritable(minValue));
    }
}