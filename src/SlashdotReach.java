/**
 * Waylin Wang, Myron Zhao
 * CPE 369 - 03, Lab 8
 * Problem 2
 */
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable; // Hadoop's serialized int wrapper class
import org.apache.hadoop.io.Text;        // Hadoop's serialized String wrapper class
import org.apache.hadoop.mapreduce.Mapper; // Mapper class to be extended by our Map function
import org.apache.hadoop.mapreduce.Reducer; // Reducer class to be extended by our Reduce function
import org.apache.hadoop.mapreduce.Job; // the MapReduce job class that is used a the driver
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; // class for "pointing" at output file
import org.apache.hadoop.conf.Configuration; // Hadoop's configuration object


import org.apache.hadoop.fs.Path;                // Hadoop's implementation of directory path/filename
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


// Exception handling

import java.io.IOException;


public class SlashdotReach extends Configured implements Tool {
    static int k = 77360;
    //To map the first time
    public static class MatMultOneMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] vals = value.toString().split("\\s+");//0 = x, 1 = y, z = 1
            if (vals.length != 2) {
                return;
            }
            for (int i = 0; i < k; i++) {
                String reduceKey = vals[0] + "," + i;
                String reduceVal = vals[1] + "," + "1";
                context.write(new Text(reduceKey), new Text(reduceVal));

                reduceKey = i + "," + vals[1];
                reduceVal = vals[0] + "," + "1";
                context.write(new Text(reduceKey), new Text(reduceVal));
            }
        } // map
    }  // mapper class

    //Second map for the results.
    public static class MatMultRepeatMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] vals = value.toString().split(",");//0 = x, 1 = y, z = 2
            for (int i = 0; i < k; i++) {
                String reduceKey = vals[0] + "," + i;
                String reduceVal = vals[1] + "," + vals[2];
                context.write(new Text(reduceKey), new Text(reduceVal));
            }
        } // map
    }
    //Second map for the original input
    public static class MatOriginalMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] vals = value.toString().split("\\s+");//0 = x, 1 = y, z = 1
            if (vals.length != 2) {
                return;
            }
            for (int i = 0; i < k; i++) {
                String reduceKey = i + "," + vals[1];
                String reduceVal = vals[0] + "," + "1";
                context.write(new Text(reduceKey), new Text(reduceVal));
            }
        } // map
    }  // mapper class

    public static class MatMultReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int first[] = new int[k];
            int second[] = new int[k];

            for (Text val : values) {
                String vals[] = val.toString().split(",");
                if (first[Integer.parseInt(vals[0].trim())] == 0) {
                    first[Integer.parseInt(vals[0].trim())] = Integer.parseInt(vals[1].trim());
                }
                else {
                    second[Integer.parseInt(vals[0].trim())] = Integer.parseInt(vals[1].trim());
                }
            }

            int sum = 0;
            for (int i = 0; i < k; i++) {
                sum += first[i] * second[i];
            }

            String emitKey = key.toString()+","+ sum;

            context.write(new Text(emitKey), new Text(""));
        }
    }

    public static class CountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text values, Context context)
                throws IOException, InterruptedException {
            String[] vals = values.toString().split(",");//0 = x, 1 = y, z = 2
            context.write(new Text(vals[0]), new IntWritable(Integer.parseInt(vals[2].trim())));
        }
    }

    public static class CountReducer extends Reducer<Text, IntWritable, Text, Text> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = -1;
            for (IntWritable val : values) {
                if (!(val.get() == 0)) {
                    sum++;
                }
            }
            context.write(new Text(sum+","+key.toString()), new Text(""));
        }
    }

    public static class SortMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        public void map(LongWritable key, Text values, Context context)
                throws IOException, InterruptedException {
            String[] vals = values.toString().split(",");//0 = sum, 1 = node number
            Long emitKey = Long.parseLong(vals[0].trim());
            context.write(new LongWritable(emitKey), new Text(vals[1]));
        }
    }

    public static class SortReducer extends Reducer<LongWritable, Text, Text, Text> {
        static int emitCount = 0;
        public void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            if (emitCount < 50) {
                for (Text value : values) {
                    if (emitCount < 50) {
                        emitCount++;
                        context.write(value, new Text("Reachable nodes: " + key.toString()));
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
        if(fs.exists(new Path("temp/data/"))){
            fs.delete(new Path("temp/data/"),true);
        }

        Job first = Job.getInstance(conf, "lab8-5-iter-0");
        first.setJarByClass(SlashdotReach.class);

        FileInputFormat.setInputPaths(first, new Path("/datasets/soc-Slashdot0811.txt")); //new Path("Lab8/data.txt"));
        FileOutputFormat.setOutputPath(first, new Path("temp/data/0"));

        first.setMapperClass(MatMultOneMapper.class);
        first.setReducerClass(MatMultReducer.class);
        first.setOutputKeyClass(Text.class);
        first.setOutputValueClass(Text.class);
        first.waitForCompletion(true);

        for (int i = 1; i < 6; i++) {
            Job mult = Job.getInstance(conf, "lab8-5-iter-" + i);
            mult.setJarByClass(SlashdotReach.class);

            MultipleInputs.addInputPath(mult, new Path("/datasets/soc-Slashdot0811.txt"), //new Path("Lab8/data.txt"),
                    TextInputFormat.class, MatOriginalMapper.class);
            MultipleInputs.addInputPath(mult, new Path("temp/data/"+ (i-1)+"/part-r-00000"),
                    TextInputFormat.class, MatMultRepeatMapper.class);
            FileOutputFormat.setOutputPath(mult, new Path("temp/data/"+i));

            mult.setReducerClass(MatMultReducer.class);
            mult.setOutputKeyClass(Text.class);
            mult.setOutputValueClass(Text.class);
            mult.waitForCompletion(true);
        }

        Job sum = Job.getInstance(conf, "lab8-5-sum");
        sum.setJarByClass(SlashdotReach.class);

        FileInputFormat.setInputPaths(sum, new Path("temp/data/5/part-r-00000"));
        FileOutputFormat.setOutputPath(sum, new Path("temp/data/6"));

        sum.setMapperClass(CountMapper.class);
        sum.setReducerClass(CountReducer.class);
        sum.setOutputKeyClass(Text.class);
        sum.setOutputValueClass(IntWritable.class);
        sum.waitForCompletion(true);

        Job sort = Job.getInstance(conf, "lab8-5-sort");
        sort.setJarByClass(SlashdotReach.class);

        FileInputFormat.setInputPaths(sort, new Path("temp/data/6/part-r-00000"));
        FileOutputFormat.setOutputPath(sort, new Path(args[0]));

        sort.setMapperClass(SortMapper.class);
        sort.setReducerClass(SortReducer.class);
        sort.setOutputKeyClass(LongWritable.class);
        sort.setOutputValueClass(Text.class);
        sort.setSortComparatorClass(LongWritable.DecreasingComparator.class);

        sort.waitForCompletion(true);

        fs.delete(new Path("temp/data/"),true);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new SlashdotReach(), args);
        System.exit(res);
    }

} // MyMapReduceDriver
