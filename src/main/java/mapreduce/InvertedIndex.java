package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class InvertedIndex {

    public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {

        //        word and url
        private Text keyInfo = new Text();
        //        word freq
        private Text valueInfo = new Text();
        //        split
        private FileSplit split;

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            split = (FileSplit) context.getInputSplit();
            System.out.println("offset" + key);
            System.out.println("val" + value);
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                keyInfo.set(itr.nextToken() + ":" + split.getPath().toString());
                valueInfo.set("1");
                context.write(keyInfo, valueInfo);
            }
            System.out.println("key" + keyInfo);
            System.out.println("value" + valueInfo);
        }
    }

    public static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {
        private Text info = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (Text value : values) {
                sum += Integer.parseInt(value.toString());
            }

            int splitIndex = key.toString().indexOf(":");

            info.set(key.toString().substring(splitIndex + 1) + ":" + sum);

            key.set(key.toString().substring(0, splitIndex));

            context.write(key, info);
            System.out.println("key" + key);
            System.out.println("value" + info);
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {

            String fileList = new String();
            for (Text value : values) {
                fileList += value.toString() + ";";
            }
            result.set(fileList);

            context.write(key, result);
        }

    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf, "mapreduce.InvertedIndex");
            job.setJarByClass(InvertedIndex.class);

            job.setMapperClass(InvertedIndexMapper.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setCombinerClass(InvertedIndexCombiner.class);
            job.setReducerClass(InvertedIndexReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

//            FileInputFormat.addInputPath(job, new Path("hdfs://192.168.52.140:9000/index/"));
//            FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.52.140:9000/out_index" + System.currentTimeMillis() + "/"));
//            input and output
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IllegalStateException | IllegalArgumentException | IOException | ClassNotFoundException | InterruptedException e) {
            e.printStackTrace();
        }

    }
}
