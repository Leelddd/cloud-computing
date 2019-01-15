package mapreduce.friend;

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
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;

/**
 * Basic friends discover with one-degree friends removed
 * Both Followed and Following is considered as a relation
 * A-B B-C => recommend C to A and A to C only if A-C is not friend
 */
public class FriendsDiscoverRemoveDup {

    public static class Job1_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] line = value.toString().split(" ");
            context.write(new Text(line[0]), new Text(line[1]));
            context.write(new Text(line[1]), new Text(line[0]));
        }
    }

    public static class Job1_Reducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Set<String> friends = new HashSet<String>();
            for (Text v : values) {
                friends.add(v.toString());
                if (key.toString().compareTo(v.toString()) < 0) {
                    context.write(new Text(key + "\t" + v), new Text("1"));
                } else {
                    context.write(new Text(v + "\t" + key), new Text("1"));
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

    public static class Job2_Mapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            context.write(new Text(line[0] + "\t" + line[1]), new Text(line[2]));
        }
    }

    public static class Job2_Reducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            boolean is_potential_friend = true;
            for (Text v : values) {
                if (v.toString().equals("1")) {
                    is_potential_friend = false;
                    break;
                }
            }
            if (is_potential_friend) {
                String[] potential_friends = key.toString().split("\t");
                context.write(new Text(potential_friends[0]), new Text(
                        potential_friends[1]));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }

        Path path = new Path(otherArgs[1]);
        FileSystem fileSystem = path.getFileSystem(conf);
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }

//        Job job1 = new Job(conf);
        Job job1 = Job.getInstance(conf, "mapreduce.friend.FriendsDiscoverRemoveDup");
        job1.setJarByClass(FriendsDiscoverRemoveDup.class);
        job1.setMapperClass(Job1_Mapper.class);
        job1.setReducerClass(Job1_Reducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
        Path tempDir = new Path("temp_"
                + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
        FileOutputFormat.setOutputPath(job1, tempDir);

        if (job1.waitForCompletion(true)) {
            Job job2 = Job.getInstance(conf, "mapreduce.friend.FriendsDiscoverRemoveDup");
            job2.setJarByClass(FriendsDiscoverRemoveDup.class);
            FileInputFormat.addInputPath(job2, tempDir);
            job2.setMapperClass(Job2_Mapper.class);
            job2.setReducerClass(Job2_Reducer.class);
            FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            FileSystem.get(conf).deleteOnExit(tempDir);
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }

}