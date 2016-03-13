/**
 * Waylin Wang, Myron Zhao
 * CPE 369 - 03, Lab 8
 * Problem 3
 */

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable; // Hadoop's serialized int wrapper class
import org.apache.hadoop.io.Text;        // Hadoop's serialized String wrapper class
import org.apache.hadoop.mapreduce.Mapper; // Mapper class to be extended by our Map function
import org.apache.hadoop.mapreduce.Reducer; // Reducer class to be extended by our Reduce function
import org.apache.hadoop.mapreduce.Job; // the MapReduce job class that is used a the driver
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; // class for "pointing" at output file
import org.apache.hadoop.conf.Configuration; // Hadoop's configuration object


import org.apache.hadoop.fs.Path;                // Hadoop's implementation of directory path/filename
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


// Exception handling

import java.io.IOException;
import java.util.*;


public class SeedKNN extends Configured implements Tool {
// Mapper  Class Template

    public static class LineMapper extends Mapper<LongWritable, Text, Text, Text> {
        static int lineNum = 1;
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            context.write(new Text(lineNum+""), value);
            lineNum++;
        } // map
    }  // mapper class
    public static class LineReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, values.iterator().next());
        }
    }

    public static class CalcMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int k = Integer.parseInt(conf.get("k"));

            String []vals = value.toString().split("\\s+");
            if (vals.length == 9) {
                String outVal = "";
                for (int i = 1; i < vals.length - 1; i++) {
                    outVal+=vals[i]+",";
                }

                for(int i = 1; i <= k; i++) {
                    if(i == Integer.parseInt(vals[0].trim())) {
                        context.write(new Text(i+""), new Text(outVal + "A"));
                    }
                    else {
                        context.write(new Text(i+""), new Text(outVal + "B," + vals[0]));
                    }
                }
            }
        } // map
    }  // mapper class

    public static class CalcReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int k = 210;
            Double[] self = new Double[7];
            Double[] calc = new Double[7];
            Double[][] storedVals = new Double[k][8];
            String selfNode = "";
            int outNode = 0;
            Map<Integer, Double> initialValues = new HashMap<>();

            for (Text value : values) {
            	String[]parsedValues = value.toString().split(",");
                if (parsedValues.length == 10) {
                    for (int i = 0; i < parsedValues.length - 2; i++) {
                    	outNode = Integer.parseInt(parsedValues[0].trim());
                        storedVals[outNode][i] = Double.parseDouble(parsedValues[i].trim());
                    }
                } else {
		            String[]parsedSelf = key.toString().split(",");
		            for (int i = 0; i < parsedSelf.length - 1; i++) {
		                self[i] = Double.parseDouble(parsedSelf[i].trim());
		            }
		            selfNode = parsedSelf[0].trim();
                }

            }

            for (int i = 0; i < k; i ++) {
            	double distance = 0;
	            for (int j = 1; j < self.length; j++) {
	                distance += Math.pow(self[j] - storedVals[i][j], 2);
	            }
	            distance = Math.sqrt(distance);
	            initialValues.put(outNode, distance);
            }

            // for (Text value : values) {
            //     String[]parsedValues = value.toString().split(",");
            //     if (parsedValues.length == 9) {
            //         for (int i = 0; i < parsedValues.length - 2; i++) {
            //             calc[i] = Double.parseDouble(parsedValues[i].trim());
            //         }
            //         int outNode = Integer.parseInt(parsedValues[8].trim());

            //         double distance = 0;
            //         for (int i = 0; i < self.length; i++) {
            //             distance += Math.pow(self[i] - calc[i], 2);
            //         }
            //         distance = Math.sqrt(distance);
            //         initialValues.put(outNode, distance);
            //     }
            // }

            Map<Integer, Double> sortedMap = new HashMap<>();
            sortedMap = CalcReducer.sortMap(initialValues);

            String outVal = "";
            Iterator iter = sortedMap.entrySet().iterator();
            for (int i = 0; i < 5; i++) {
                Map.Entry pair = (Map.Entry) iter.next();
                outVal+="("+pair.getKey() + ", " + pair.getValue() + "), ";
            }

            outVal = outVal.substring(0, outVal.length() - 2);

            context.write(new Text(selfNode), new Text(outVal));

        }

        public static <K, V extends Comparable<? super V>> Map<K, V> sortMap(final Map<K, V> mapToSort) {
            List<Map.Entry<K, V>> entries = new ArrayList<Map.Entry<K, V>>(mapToSort.size());

            entries.addAll(mapToSort.entrySet());

            Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {
                @Override
                public int compare(final Map.Entry<K, V> entry1, final Map.Entry<K, V> entry2) {
                    return entry1.getValue().compareTo(entry2.getValue());
                }
            });

            Map<K, V> sortedMap = new LinkedHashMap<K, V>();
            for (Map.Entry<K, V> entry : entries) {
                sortedMap.put(entry.getKey(), entry.getValue());
            }
            return sortedMap;
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = super.getConf();
        conf.set("k", "210");

        FileSystem fs = FileSystem.get(conf);

        if(fs.exists(new Path(args[0]))){
            fs.delete(new Path(args[0]), true);
        }
        if (fs.exists(new Path("temp/data/"))) {
            fs.delete(new Path("temp/data/"), true);
        }

        Job addLineNum = Job.getInstance(conf, "lab8-3");

        addLineNum.setJarByClass(SeedKNN.class);

        FileOutputFormat.setOutputPath(addLineNum, new Path("temp/data/"));
        FileInputFormat.setInputPaths(addLineNum, new Path("/datasets/seeds_dataset.txt"));

        addLineNum.setMapperClass(LineMapper.class);
        addLineNum.setReducerClass(LineReducer.class);
        addLineNum.setOutputKeyClass(Text.class);
        addLineNum.setOutputValueClass(Text.class);
        addLineNum.waitForCompletion(true);

        Job calc = Job.getInstance(conf, "lab8-3");

        calc.setJarByClass(SeedKNN.class);

        FileOutputFormat.setOutputPath(calc, new Path(args[0]));
        FileInputFormat.setInputPaths(calc, new Path("temp/data/part-r-00000"));

        calc.setMapperClass(CalcMapper.class);
        calc.setReducerClass(CalcReducer.class);
        calc.setOutputKeyClass(Text.class);
        calc.setOutputValueClass(Text.class);
        calc.waitForCompletion(true);

        fs.delete(new Path("temp/"), true);


        return 0;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new SeedKNN(), args);
        System.exit(res);
    }

} // MyMapReduceDriver



