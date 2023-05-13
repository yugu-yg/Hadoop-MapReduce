/**
 * @Author: Yu Gu
 * Andrew ID: ygu3
 */

package org.myorg;

import java.io.IOException;
import java.util.StringTokenizer;
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

public class FindPattern extends Configured implements Tool {

    // Mapper class for the WordCount class which inputs key:value pair in LongWritable and Text
    // and output key:value pair in Text and NullWritable
    public static class FindPatternMap extends Mapper<LongWritable, Text, Text, NullWritable>
    {
        // Stores the tokenized word
        private Text word = new Text();

        /**
         * Function to perform Hadoop's map operation. It tokenizes the words in the input
         * file and assigns NullWritable value to each of them.
         * @param key LongWritable key as input to the map function
         * @param value Text as value input to the map function
         * @param context Context to form output of map function
         * @throws IOException Exception while performing IO operations
         * @throws InterruptedException Exception during interrupted exception
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            // Convert value to string
            String line = value.toString();

            // Tokenize line
            StringTokenizer tokenizer = new StringTokenizer(line);

            // Loop over all the tokens
            while(tokenizer.hasMoreTokens())
            {
                // Set key for the output
                word.set(tokenizer.nextToken());

                // Set context - Key:Value pair
                context.write(word, NullWritable.get());
            }
        }
    }

    // Reducer class for the WordCount class which inputs key:value pair in Text and NullWritable
    // and output key:value pair in Text and NullWritable
    public static class FindPatternReducer extends Reducer<Text, NullWritable, Text, NullWritable>
    {
        /**
         * Function to perform Hadoop's reduce operation. It gets the key:value pairs from the
         * mapper function, reduces them to find the words in the input file that contain
         * the word "cool" (case-insensitive)
         * @param key Text as input key to the reduce function
         * @param values Iterable<NullWritable> as values to the reduce function
         * @param context Context to form output of reduce function
         * @throws IOException Exception while performing IO operations
         * @throws InterruptedException Exception during interrupted exception
         */
        public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException
        {

            if (Pattern.compile(Pattern.quote("cool"), Pattern.CASE_INSENSITIVE).matcher(key.toString()).find()) {
                context.write(key, NullWritable.get());
            }
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
        job.setJarByClass(FindPattern.class);

        // Set name of job
        job.setJobName("findpattern");

        // Set output key's class
        job.setOutputKeyClass(Text.class);

        // Set output value's class
        job.setOutputValueClass(NullWritable.class);

        // Set mapper class
        job.setMapperClass(FindPatternMap.class);

        // Set combiner class
        job.setCombinerClass(FindPatternReducer.class);

        // Set reducer class
        job.setReducerClass(FindPatternReducer.class);

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
        int result = ToolRunner.run(new FindPattern(), args);
        System.exit(result);
    }
}