import java.io.*;
import java.util.*;
import java.lang.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class StateStats {

    //These are the words we are searching for
    private static List<String> search_for = Arrays.asList("education", "politics",
        "sports", "agriculture");

    //This mapper handles the HTML files we were given
    public static class TokenizerMapper extends
        Mapper<Object, Text, Text, IntWritable> {

        private String state = new String();

        //This goes through our files line-by-line
        public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

                //Strip the HTML out of the line
                String nohtml = value.toString().replaceAll("\\<.*?>", "");

                //Go through each word and see if it is in our list
                for (String word : nohtml.split(" ")) {
                    if (search_for.contains(word)) {
                        context.write(new Text(state + ":" + word),
                            new IntWritable(1));
                    }
                }
        }

        //Get the state from the filename
        public void setup(Context context) throws java.io.IOException,
            java.lang.InterruptedException {

            String fileName = ((FileSplit) context.getInputSplit()).getPath().toString();
            state = fileName.substring(fileName.lastIndexOf("/")+1);
        }
    }

    //This reducer sums up integers with the same key
    public static class IntSumReducer extends
        Reducer<Text,IntWritable,Text,IntWritable> {
        
        public void reduce(Text key, Iterable<IntWritable> values,
            Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    //this mapper maps by word from the first operation giving
    //state:occurences as the value
    public static class WordMapper extends
        Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

            //parse the line
            String[] parts = value.toString().split(":|\t");
            String state = parts[0];
            String word = parts[1];
            String occurences = parts[2];

            context.write(new Text(word), new Text(state + ":" + occurences));
        }
    }

    //This reducer combines finds the states with the highest word occurences
    public static class WordReducer extends
        Reducer<Text,Text,Text,Text> {

        //used to keep track of a state and occurence pair
        class StateOccurences {
            String state;
            int occurences;

            StateOccurences(String state, int occurences) {
                this.state = state;
                this.occurences = occurences;
            }
        }

        //used to compare StateOccurences for sorting in descending order
        class SortByOccurences implements Comparator<StateOccurences> {
            public int compare(StateOccurences a, StateOccurences b) {
                return b.occurences - a.occurences;
            }
        }

        //find the state with the max occurences of the word and print it out
        public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
            
            ArrayList<StateOccurences> list = new ArrayList<StateOccurences>();
            for (Text val : values) {
                String[] parts = val.toString().split(":");
                String state = parts[0];
                int occurences = Integer.parseInt(parts[1]);
                list.add(new StateOccurences(state, occurences));
            }

            Collections.sort(list, new SortByOccurences());
            context.write(key, new Text(list.get(0).state));
        }
    }

    //this mapper maps by state name from the first operation giving
    //word:occurences as the value
    public static class StateKeyMapper extends
        Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

            //parse the line
            String[] parts = value.toString().split("\t");
            String our_key = parts[0];
            int our_value = Integer.parseInt(parts[1]);
            String[] parts2 = our_key.split(":");
            String state = parts2[0];
            String word = parts2[1];

            context.write(new Text(state), new Text(word + ":" + our_value));
        }
    }

    //This reducer creates Ranking strings for States
    public static class RankingReducer extends
        Reducer<Text,Text,Text,Text> {

        //a pairing of a word and its occurences
        class WordOccurences {
            String word;
            int occurences;
            
            WordOccurences(String word, int occurences) {
                this.word = word;
                this.occurences = occurences;
            }
        }

        //used to compair WordOccurences for sorting in descending order
        class SortByOccurences implements Comparator<WordOccurences> {
            public int compare(WordOccurences a, WordOccurences b) {
                return b.occurences - a.occurences;
            }
        }

        public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

            //go through all the words for each state and keep track of
            //occurences
            ArrayList<WordOccurences> list = new ArrayList<WordOccurences>();
            for (Text val : values) {
                String[] parts = val.toString().split(":");
                String word = parts[0];
                int occurences = Integer.parseInt(parts[1]);
                list.add(new WordOccurences(word, occurences));
            }

            //sort and create ranking output
            Collections.sort(list, new SortByOccurences());
            String ranking = "";
            for (WordOccurences wo : list) {
                ranking += wo.word + ">";
            }
            ranking = ranking.substring(0, ranking.length() - 1);

            //print it out
            context.write(new Text(ranking), new Text(key));
        }
    }

    //this mapper maps by ranking String with the value being the state
    public static class RankingMapper extends
        Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

            //parse the line
            String[] parts = value.toString().split("\t");
            String ranking = parts[0];
            String state = parts[1];

            context.write(new Text(ranking), new Text(state));
        }
    }

    //This reducer combines states under ranking strings
    public static class CombiningRankingReducer extends
        Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

            ArrayList<String> states = new ArrayList<String>();
            for (Text state : values) {
                states.add(state.toString());
            }

            context.write(key, new Text(String.join(",", states)));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Path in1Path = new Path(args[0]);
        Path out1Path = new Path("output_statewise");
        Path out2Path = new Path("output_dominant");
        Path out3Path = new Path("output_ranking1");
        Path out4Path = new Path("output_ranking2");

        //First we count how many times the words are used for each state
        Job job1 = Job.getInstance(conf, "Statewise");
        job1.setJarByClass(StateStats.class);
        job1.setMapperClass(TokenizerMapper.class);
        job1.setCombinerClass(IntSumReducer.class);
        job1.setReducerClass(IntSumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, in1Path);
        FileOutputFormat.setOutputPath(job1, out1Path);
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        //using previous output, map by word and find max state in reduce
        Job job2 = Job.getInstance(conf, "Dominant States");
        job2.setJarByClass(StateStats.class);
        job2.setMapperClass(WordMapper.class);
        job2.setReducerClass(WordReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, out1Path);
        FileOutputFormat.setOutputPath(job2, out2Path);
        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }

        //map by state and create a ranking String for every state
        Job job3 = Job.getInstance(conf, "Same Ranking Step 1");
        job3.setJarByClass(StateStats.class);
        job3.setMapperClass(StateKeyMapper.class);
        job3.setReducerClass(RankingReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, out1Path);
        FileOutputFormat.setOutputPath(job3, out3Path);
        if (!job3.waitForCompletion(true)) {
            System.exit(1);
        }

        //map by ranking string and create add states together
        Job job4 = Job.getInstance(conf, "Same Ranking Step 2");
        job4.setJarByClass(StateStats.class);
        job4.setMapperClass(RankingMapper.class);
        job4.setReducerClass(CombiningRankingReducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job4, out3Path);
        FileOutputFormat.setOutputPath(job4, out4Path);
        if (!job4.waitForCompletion(true)) {
            System.exit(1);
        }
    }
}
