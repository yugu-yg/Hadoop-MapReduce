/**
 * @Author: Yu Gu
 * Andrew ID: ygu3
 */

package edu.cmu.andrew.mm6;


import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Objects;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import java.util.regex.Pattern;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RapesPlusRobberies extends Configured implements Tool {

    // Mapper class for the RapesPlusRobberies class which inputs key:value pair in LongWritable and
    // Text and output key:value pair in Text and IntWritable
    public static class RapesPlusRobberiesMap extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        // Stores the value corresponding to each crime that was either
        // a rape or a robbery. It is initialized to one.
        private final static IntWritable one = new IntWritable(1);

        // Stores the output key from mapper, which is a simple text line
        private Text word = new Text();

        /**
         * Function to perform Hadoop's map operation. It finds the crimes that
         * were classified as rapes and robberies and assigns each one of them a
         * value of one.
         * @param key LongWritable key as input to the map function
         * @param value Text as value input to the map function
         * @param context Context to form output of map function
         * @throws IOException Exception while performing IO operations
         * @throws InterruptedException Exception during interrupted exception
         */
        // Source: https://stackoverflow.com/questions/16198752/advantages-of-using-nullwritable-in-hadoop
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            // Convert value to string
            String line = value.toString();

            // If the crime was a RAPE or a ROBBERY
            if (Objects.equals(line.split("\t")[4], "ROBBERY") || Objects.equals(line.split("\t")[4], "RAPE")) {

                // Update content of key
                word.set("Total rapes and robberies");

                // Set context - Key:Value pair
                context.write(word, one);
            }
        }
    }

    // Reducer class for the RapesPlusRobberies class which inputs key:value pair in Text and
    // IntWritable and output key:value pair in Text and IntWritable
    public static class RapesPlusRobberiesReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        /**
         * Function to perform Hadoop's reduce operation. It gets the key:value pairs from the
         * mapper function, reduces the output from mapper to find the count of
         * the number of rapes and robberies that occurred in the input data.
         * @param key Text as input key to the reduce function
         * @param values Iterable<IntWritable> as values to the reduce function
         * @param context Context to form output of reduce function
         * @throws IOException Exception while performing IO operations
         * @throws InterruptedException Exception during interrupted exception
         */
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            // Stores count of word in input file
            int sum = 0;

            // Loop over values
            for(IntWritable value: values)
            {
                // Increment sum
                sum += value.get();
            }

            // Set context as Key:Value pair storing sample string and count of rapes and robberies
            context.write(key, new IntWritable(sum));
        }
    }

    /**
     * Function to run the Hadoop map reduce operations. It also sets
     * various configurations required for the execution
     * @param args Input arguments to the Map Reduce function
     * @return Status code of the execution - 1 = Success; 0 = Failure
     * @throws Exception Exception if it occurs during execution
     */
    public int run(String[] args) throws Exception  {

        // Create a new job object
        Job job = new Job(getConf());

        // Set class for the job
        job.setJarByClass(RapesPlusRobberies.class);

        // Set name of job
        job.setJobName("rapesplusrobberies");

        // Set output key's class
        job.setOutputKeyClass(Text.class);

        // Set output value's class
        job.setOutputValueClass(IntWritable.class);

        // Set mapper class
        job.setMapperClass(RapesPlusRobberiesMap.class);

        // Set combiner class
        job.setCombinerClass(RapesPlusRobberiesReducer.class);

        // Set reducer class
        job.setReducerClass(RapesPlusRobberiesReducer.class);

        // Set input format class
        job.setInputFormatClass(TextInputFormat.class);

        // Set output format class
        job.setOutputFormatClass(TextOutputFormat.class);

        // Set input file path
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // Set output file path
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Stores a boolean value stating if the program executed successfully
        boolean success = job.waitForCompletion(true);

        // Return program execution status
        return success ? 0: 1;
    }


    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub

        // Run the Hadoop map reduce operation, store the status of program execution and display it
        int result = ToolRunner.run(new RapesPlusRobberies(), args);
        System.exit(result);
    }

}