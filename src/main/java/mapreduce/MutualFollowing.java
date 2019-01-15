package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class MutualFollowing {

    public static class MutualFollowingMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] line = value.toString().split(" ");
            if (line[0].compareTo(line[1]) < 0) {
                context.write(new Text(line[0] + "\t" + line[1]), new Text("0"));
            } else {
                context.write(new Text(line[1] + "\t" + line[0]), new Text("1"));
            }
        }
    }

    public static class MutualFollowingReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            Text t = null;
            for (Text v : values) {
                t = v;
                count++;
            }
            if (count == 2) {
                context.write(key, new Text("2"));
            } else if (count == 1) {
                if (t == null) {
                    throw new IOException();
                }
                context.write(key, t);
            }
        }
    }

    public static class FriendsDiscoverMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            context.write(new Text(line[0]), new Text(line[1] + "\t" + line[2]));
        }
    }

    public static class FriendsDiscoverReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Set<String> friends = new HashSet<>();
            for (Text v : values) {
                String[] line = v.toString().split("\t");
                if (line[1].equals("2")) {
                    friends.add(line[0]);
                } else {
                    context.write(new Text(key + "\t" + line[0]), new Text(line[1]));
                }
            }
            List<String> potential_friends = new ArrayList<>(friends);
            for (int i = 0; i < potential_friends.size(); i++) {
                for (int j = 0; j < potential_friends.size(); j++) {
                    if (potential_friends.get(i).compareTo(
                            potential_friends.get(j)) < 0) {
                        context.write(new Text(potential_friends.get(i) + "\t"
                                + potential_friends.get(j)), new Text("2"));
                    }
                }
            }
        }
    }

    public static class RemoveDupMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            context.write(new Text(line[0] + "\t" + line[1]), new Text(line[2]));
        }
    }

    public static class RemoveDupReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            boolean A2B = true;
            boolean B2A = true;
            boolean mutual = false;
            for (Text v : values) {
                if (v.toString().equals("0")) {
                    A2B = false;
                } else if (v.toString().equals("1")) {
                    B2A = false;
                } else if (v.toString().equals("2")) {
                    mutual = true;
                }
            }

            if (mutual) {
                String[] potential_friends = key.toString().split("\t");
                if (A2B) {
                    context.write(new Text(potential_friends[0]), new Text(potential_friends[1]));
                }
                if (B2A) {
                    context.write(new Text(potential_friends[1]), new Text(potential_friends[0]));
                }
            }
        }
    }

    public static Path tmp() {
        return new Path("temp_" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
    }

    public static Path startAJob(Configuration conf, Class<?> jar, Class<? extends Mapper> mapper,
                                 Class<? extends Reducer> reducer, Path input, Path output) throws IOException {
        Job job1 = Job.getInstance(conf, "mapreduce.MutualFollowing");
        job1.setJarByClass(jar);
        job1.setMapperClass(mapper);
        job1.setReducerClass(reducer);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, input);
        Path outputDir1 = tmp();
        FileOutputFormat.setOutputPath(job1, tmp());
        return outputDir1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Path path = new Path(args[1]);
        FileSystem fileSystem = path.getFileSystem(conf);
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }

        Job job1 = Job.getInstance(conf, "mapreduce.MutualFollowing");
        job1.setJarByClass(MutualFollowing.class);
        job1.setMapperClass(MutualFollowingMapper.class);
        job1.setReducerClass(MutualFollowingReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        Path outputDir1 = tmp();
        System.out.println(outputDir1.toString());
        FileOutputFormat.setOutputPath(job1, outputDir1);

        if (job1.waitForCompletion(true)) {
            Job job2 = Job.getInstance(conf, "mapreduce.MutualFollowing");
            job2.setJarByClass(MutualFollowing.class);
            job2.setMapperClass(FriendsDiscoverMapper.class);
            job2.setReducerClass(FriendsDiscoverReducer.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job2, outputDir1);
            Path outputDir2 = tmp();
            System.out.println(outputDir2.toString());
            FileOutputFormat.setOutputPath(job2, outputDir2);

            if (job2.waitForCompletion(true)) {
                Job job3 = Job.getInstance(conf, "mapreduce.MutualFollowing");
                job3.setJarByClass(MutualFollowing.class);
                job3.setMapperClass(RemoveDupMapper.class);
                job3.setReducerClass(RemoveDupReducer.class);
                job3.setOutputKeyClass(Text.class);
                job3.setOutputValueClass(Text.class);

                FileInputFormat.addInputPath(job3, outputDir2);
                FileOutputFormat.setOutputPath(job3, new Path(args[1]));

//                FileSystem.get(conf).deleteOnExit(outputDir1);
                System.exit(job3.waitForCompletion(true) ? 0 : 1);
            }
        }
    }


}
