/**
 * Waylin Wang, Myron Zhao
 * CPE 369 - 03, Lab 8
 * Problem 1
 */

import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable; // Hadoop's serialized int wrapper class
import org.apache.hadoop.io.Text;        // Hadoop's serialized String wrapper class
import org.apache.hadoop.mapreduce.Mapper; // Mapper class to be extended by our Map function
import org.apache.hadoop.mapreduce.Reducer; // Reducer class to be extended by our Reduce function
import org.apache.hadoop.mapreduce.Job; // the MapReduce job class that is used a the driver
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; // class for  standard text input
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs; // class for "pointing" at input file(s)
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; // class for "pointing" at output file
import org.apache.hadoop.conf.Configuration; // Hadoop's configuration object


import org.apache.hadoop.fs.Path;                // Hadoop's implementation of directory path/filename
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


// Exception handling

import java.io.IOException;


public class CommonHands extends Configured implements Tool {
// Mapper  Class Template

    public static class FirstMapper extends Mapper<LongWritable, Text, Text, Text> {

        static String[] suitName = {"Hearts", "Spades", "Diamonds", "Clubs"};
        static int[] outputIterations = {1, 2, 3, 1, 2, 4, 1, 2, 5, 1, 3, 4, 1, 3, 5, 1, 4, 5,
                2, 3, 4, 2, 3, 5, 2, 4, 5, 3, 4, 5};


        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            ArrayList<String> cards = new ArrayList<>();

            String []values = value.toString().split(",");
            if (values.length == 11) { //to make sure tha we get an actual line
                Double []intVals = new Double[values.length];
                for (int i = 0; i < values.length; i++) {
                    intVals[i] = Double.parseDouble(values[i]);
                }
                for (int i = 0; i < intVals.length - 1; i += 2) {
                    String cardVal = "(" + suitName[intVals[i].intValue() - 1] + " " + intVals[i+1].intValue() + ")";
                    cards.add(cardVal);
                }
                Collections.sort(cards);

                for (int i = 0; i < outputIterations.length - 2; i += 3) {
                    String emitKey = cards.get(outputIterations[i] - 1) + " " +
                                        cards.get(outputIterations[i + 1] - 1) + " " +
                                        cards.get(outputIterations[i + 2] - 1);
                    context.write(new Text(emitKey), new Text("A"));
                }
            }
        } // map
    }  // mapper class


    public static class SecondMapper extends Mapper<LongWritable, Text, Text, Text> {
        static String[] suitName = {"Hearts", "Spades", "Diamonds", "Clubs"};
        static int[] outputIterations = {1, 2, 3, 1, 2, 4, 1, 2, 5, 1, 3, 4, 1, 3, 5, 1, 4, 5,
                2, 3, 4, 2, 3, 5, 2, 4, 5, 3, 4, 5};

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            ArrayList<String> cards = new ArrayList<>();
            String []values = value.toString().split(",");
            if (values.length == 11) { //to make sure tha we get an actual line
                Double []intVals = new Double[values.length];
                for (int i = 0; i < values.length; i++) {
                    intVals[i] = Double.parseDouble(values[i]);
                }
                for (int i = 0; i < intVals.length - 1; i += 2) {
                    String cardVal = "(" + suitName[intVals[i].intValue() - 1] + " " + intVals[i+1].intValue() + ")";
                    cards.add(cardVal);
                }
                Collections.sort(cards);

                for (int i = 0; i < outputIterations.length - 2; i += 3) {
                    String emitKey = cards.get(outputIterations[i] - 1) + " " +
                            cards.get(outputIterations[i + 1] - 1) + " " +
                            cards.get(outputIterations[i + 2] - 1);
                    context.write(new Text(emitKey), new Text("B"));
                }
            }
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
                int aCount = 0;
                int bCount = 0;

                for (Text value : values) {
                    if (value.toString().equals("A")) {
                        aCount++;
                    } else {
                        bCount++;
                    }
                }

                String totalVal = Math.min(aCount, bCount) + ",";

                context.write(new Text(""), new Text(totalVal + key.toString()));
        }
    }

    public static class SortMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        public void map(LongWritable key, Text values, Context context)
                throws IOException, InterruptedException {
            String value[] = values.toString().split(",");
            if (value.length == 2) {
                context.write(new LongWritable(Long.parseLong(value[0].trim())), new Text(value[1]));
            }
        }
    }

    public static class SortReducer extends Reducer<LongWritable, Text, Text, Text> {
        static int emitCount = 0;
        public void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            if (emitCount < 20) {
                for (Text value : values) {
                    if (emitCount < 20) {
                        emitCount++;
                        context.write(value, new Text(key.toString()));
                    }
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = super.getConf();
        FileSystem fs = FileSystem.get(conf);

        if(fs.exists(new Path(args[0]))){
            fs.delete(new Path(args[0]),true);
        }

        Job job = Job.getInstance(conf, "lab8-1");

        job.setJarByClass(CommonHands.class);
        MultipleInputs.addInputPath(job, new Path("/datasets/poker-hand-testing.data.txt"),
                TextInputFormat.class, FirstMapper.class ); // put what you need as input file
        MultipleInputs.addInputPath(job, new Path("/datasets/poker-hand-traning.true.data.txt"),
                TextInputFormat.class, SecondMapper.class ); // put what you need as input file

        if(fs.exists(new Path("temp/Data/"))){
            fs.delete(new Path("temp/Data/"),true);
        }

        FileOutputFormat.setOutputPath(job, new Path("temp/Data/"));
        job.setReducerClass(JoinReducer.class);

        job.setOutputKeyClass(Text.class); // specify the output class (what reduce() emits) for key
        job.setOutputValueClass(Text.class); // specify the output class (what reduce() emits) for value

        job.waitForCompletion(true);

        //Part 2 of chain
        Job sortTop20 = Job.getInstance(conf, "lab8-1-part2");
        sortTop20.setJarByClass(CommonHands.class);
        sortTop20.setMapperClass(SortMapper.class);
        sortTop20.setReducerClass(SortReducer.class);

        sortTop20.setOutputKeyClass(LongWritable.class);
        sortTop20.setOutputValueClass(Text.class);

        //shuffle in reverse
        sortTop20.setSortComparatorClass(LongWritable.DecreasingComparator.class);

        FileInputFormat.addInputPath(sortTop20, new Path("temp/Data/part-r-00000"));
        FileOutputFormat.setOutputPath(sortTop20, new Path(args[0]));

        int retVal = sortTop20.waitForCompletion(true) ? 0 : 1;

        if(fs.exists(new Path("temp/Data/"))){
            fs.delete(new Path("temp/Data/"),true);
        }

        return retVal;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new CommonHands(), args);
        System.exit(res);
    }

} // MyMapReduceDriver



