/**
 * Waylin Wang, Myron Zhao
 * CPE 369 - 03, Lab 8
 * Problem 2
 */

import java.util.ArrayList;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable; // Hadoop's serialized int wrapper class
import org.apache.hadoop.io.Text;        // Hadoop's serialized String wrapper class
import org.apache.hadoop.mapreduce.Mapper; // Mapper class to be extended by our Map function
import org.apache.hadoop.mapreduce.Reducer; // Reducer class to be extended by our Reduce function
import org.apache.hadoop.mapreduce.Job; // the MapReduce job class that is used a the driver
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; // class for  standard text input
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs; // class for "pointing" at input file(s)
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; // class for "pointing" at output file
import org.apache.hadoop.conf.Configuration; // Hadoop's configuration object


import org.apache.hadoop.fs.Path;                // Hadoop's implementation of directory path/filename
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


// Exception handling

import java.io.IOException;


public class LargestValue extends Configured implements Tool {
// Mapper  Class Template

    public static class FirstMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            ArrayList<String> cards = new ArrayList<>();

            String []values = value.toString().split(",");
            if (values.length == 11) { //to make sure tha we get an actual line
                Double []intVals = new Double[values.length];
                for (int i = 0; i < values.length; i++) {
                    intVals[i] = Double.parseDouble(values[i]);
                }
                double sum = 0;
                for (int i = 1; i < intVals.length - 1; i += 2) {
                    if (intVals[i] == 1) {
                        sum += 14;
                    }
                    else {
                        sum += intVals[i];
                    }
                }

                context.write(new LongWritable(new Double(sum).longValue()), new Text("A"));
            }
        } // map
    }  // mapper class


    public static class SecondMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            ArrayList<String> cards = new ArrayList<>();

            String []values = value.toString().split(",");
            if (values.length == 11) { //to make sure tha we get an actual line
                Double []intVals = new Double[values.length];
                for (int i = 0; i < values.length; i++) {
                    intVals[i] = Double.parseDouble(values[i]);
                }
                double sum = 0;
                for (int i = 1; i < intVals.length - 1; i += 2) {
                    if (intVals[i] == 1) {
                        sum += 14;
                    }
                    else {
                        sum += intVals[i];
                    }
                }

                context.write(new LongWritable(new Double(sum).longValue()), new Text("B"));
            }
        } // map
    }

    public static class JoinReducer extends Reducer<LongWritable, Text, Text, Text> {
        static int count = 0;
        public void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            if (count == 1) {
                return;
            }

            int aCount = 0;
            int bCount = 0;
            for (Text value : values) {
                if (value.toString().equals("A")) {
                    aCount++;
                }
                else {
                    bCount++;
                }
            }

            String emitKey = "";
            String emitVal = "";

            if (aCount < bCount) {
                emitKey = "poker-hand-traning.true.data.txt has more of the highest hands.";
                emitVal = "Value: " + key + "\tCount in this file: " + bCount + "\tCount in other file: " + aCount;
            }
            else if (aCount > bCount) {
                emitKey = "poker-hand-testing.data.txt has more of the highest hands.";
                emitVal = "Value: " + key + "\tCount in this file: " + aCount + "\tCount in other file: " + bCount;
            }
            else {
                emitKey = "Both files had the same amount of the highest hands.";
                emitVal = "Value: " + key + "\tCount in both files: " + aCount;
            }

            context.write(new Text(emitKey), new Text(emitVal));
            count++;
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = super.getConf();
        FileSystem fs = FileSystem.get(conf);

        if(fs.exists(new Path(args[0]))){
            fs.delete(new Path(args[0]),true);
        }

        Job job = Job.getInstance(conf, "lab8-2");

        job.setJarByClass(LargestValue.class);
        MultipleInputs.addInputPath(job, new Path("/datasets/poker-hand-testing.data.txt"),
                TextInputFormat.class, FirstMapper.class ); // put what you need as input file
        MultipleInputs.addInputPath(job, new Path("/datasets/poker-hand-traning.true.data.txt"),
                TextInputFormat.class, SecondMapper.class ); // put what you need as input file

        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        job.setReducerClass(JoinReducer.class);

        job.setSortComparatorClass(LongWritable.DecreasingComparator.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new LargestValue(), args);
        System.exit(res);
    }

} // MyMapReduceDriver



