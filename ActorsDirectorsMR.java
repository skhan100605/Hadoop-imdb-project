import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.Partitioner;

import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;
public class ActorsDirectorsMR {
    static Logger logger = Logger.getLogger(ActorsDirectorsMR.class);
    public static class TitleMapper extends Mapper<Object,Text,Text,Text>{
        private final static Text outputVal = new Text();
        private Text word=new Text();

        public void map(Object key,Text value, Context context) throws IOException,InterruptedException{
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                String str = itr.nextToken();
                String[] result = str.toString().split("\t");
                int n = result.length;

                String newKey= key.toString();
    
                String titleId = result[0];
                String titleType="";
                if(result[1].matches("movie")){
                    titleType=result[1];
                }
                String primaryTitle = result[2];
                //String originalTitle = result[3];  
                String startYear = result[5]; 
                String pattern ="-?\\d+";
                int temp=0;
                if(startYear.matches("-?\\d+")){
                    temp=Integer.parseInt(startYear); 
                }
                //String endYear = result[6];
                String res_val="";
                if(titleType!="" && temp>=1993 && temp<=2003){
                res_val= "titleVal"+":"+titleId+":"+titleType+":"+primaryTitle+":"+startYear; 
                }    
                //int temp = Integer.parseInt(startYear)  ;
    
                if (!titleId.equals("\\N") && !primaryTitle.equals("\\N") && !startYear.equals(("\\N"))) {
                    String  midKey = titleId;
                    word.set(midKey);
                    outputVal.set(res_val);
                    context.write(word, outputVal);
                }
    
            }
        }

    }
    public static class ActorMapper extends Mapper<Object,Text,Text,Text>{
        private final static Text outputVal = new Text();
        private Text word=new Text();

        public void map(Object key,Text value, Context context) throws IOException,InterruptedException{
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                String str = itr.nextToken();
                String[] result = str.toString().split(",");
                int n = result.length;
                String res_val="";
                String actorId="";
                String actorName="";
    
                String titleId = result[0];
                if(n>2){
                    actorId = result[1];
                    actorName = result[2]; 
                    res_val = "actorVal"+":" +titleId + ":"+ actorId+ ":"+ actorName;  
                }
               
                         
    
                if (!titleId.equals("\\N") && !actorId.equals("\\N") && !actorName.equals("\\N")) {
                    String  midKey = titleId;
                    word.set(midKey);
                    outputVal.set(res_val);
                    context.write(word, outputVal);
                }
    
            }
        }
        
    }
    public static class DirectorMapper extends Mapper<Object,Text,Text,Text>{
        private final static Text outputVal = new Text();
        private Text word=new Text();

        public void map(Object key,Text value, Context context) throws IOException,InterruptedException{
            System.out.println("hello");
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                String str = itr.nextToken();
                System.out.println(str);
                String[] result = str.toString().split("\t");
                int n = result.length;
                System.out.println(result);
                String res_val="";
                String titleId = result[0];
                String directorId="";
                if(n>1){
                    directorId = result[1];  
                    res_val ="directorVal"+":"+titleId+":"+directorId; 
                }
             
    
                if (!titleId.equals("\\N") && !directorId.equals("\\N")) {
                    String  midKey = titleId;
                    word.set(midKey);
                    outputVal.set(res_val);
                    context.write(word, outputVal);
                }
    
            }
        }
    }
    public static class MyPartitioner extends Partitioner<Text,Text>{
        public int getPartition(Text key, Text value, int numReduceTasks){
        if(numReduceTasks==0){
        return 0;
        }
        if(numReduceTasks==1){
        return 1%numReduceTasks;
        }
        if(numReduceTasks==2){
        return 2%numReduceTasks;
        }
        return 1;
        }
        }
    

    public static class JoinReducer1 extends Reducer<Text,Text,Text,Text>{
        private Text result=new Text();
        private Text joinKey = new Text();
        private static final Log LOG = LogFactory.getLog(JoinReducer1.class);
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> table1 = new ArrayList<String>();
			ArrayList<String> table2 = new ArrayList<String>();
			ArrayList<String> table3 = new ArrayList<String>();
            for (Text p : values) {
				String[] s = p.toString().split(":");
				if (s != null && s[0].trim().equals("titleVal")) {
					table1.add(p.toString());
				} else if (s != null && s[0].trim().equals("directorVal")) {
					table2.add(p.toString());
				}
                else if (s != null && s[0].trim().equals("actorVal")) {
					table3.add(p.toString());
				}
                String strData = "";
                for(int k=0; k<table1.size();k++){
                for (int i = 0; i < table2.size(); i++) {

                    for (int j = 0; j < table3.size(); j++) {
    
                        String res1[]=table2.get(i).toString().split(":");
                        String res2[]=table3.get(j).toString().split(":");
                        String res3[]=table1.get(k).toString().split(":");
                        //String res3[];
                        if(res1[2].contains(",")){
                           if(res1[2].contains(res2[2]) && (res1[1].matches(res3[1])&& res2[1].matches(res3[1])))
                           {
                               strData=" directorId: "+res1[2]+" actorId: "+res2[2]+" actorName: "+res2[3]+" titleName: "+res3[3]+" titleType: "+res3[2]+" startYear: "+res3[4];
                               result.set(strData);
                               context.write(key, result);
                               result.clear();
                           }
                     
                        }
                        else{
                        if(res1[2].matches(res2[2]) && (res1[1].matches(res3[1])&& res2[1].matches(res3[1])))
                        {
                            strData="directorId: "+res1[2]+" actorId: "+res2[2]+" actorName: "+res2[3]+" titleName: "+res3[3]+" titleType: "+res3[2]+" startYear: "+res3[4];
                            result.set(strData);
                            context.write(key, result);
                            result.clear();
                        }
                    
                    }
                        //strData=res1[2]+ "_" +res2[2];
                        
                        
                        
                       
                    }
                }
            }

			}
        }
}


    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        int split = 700*1024*1024; // This is in bytes
        String splitsize= Integer.toString(split);
        conf.set("mapreduce.input.fileinputformat.split.minsize",splitsize);
        BasicConfigurator.configure();
        logger.info("Entering application.");
        System.out.println(args[0]);
        System.out.println(args[1]);
        System.out.println(args.length);
        // conf.set("mapreduce.map.memory.mb", "2048"); // This is in Mb
        // conf.set("mapreduce.reduce.memory.mb", "2048");
        Job job1=Job.getInstance(conf,"actor-directorgig");
        job1.setJarByClass(ActorsDirectorsMR.class);
        job1.setNumReduceTasks(2);
        logger.info("setting input path");
        MultipleInputs.addInputPath(job1,new Path(args[0]),TextInputFormat.class,TitleMapper.class);
        MultipleInputs.addInputPath(job1,new Path(args[1]),TextInputFormat.class,ActorMapper.class);
       MultipleInputs.addInputPath(job1,new Path(args[2]),TextInputFormat.class,DirectorMapper.class);
       logger.info("inout path finishhed");
      job1.setPartitionerClass(MyPartitioner.class);
        job1.setReducerClass(JoinReducer1.class);
        logger.info("setting output class");
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        logger.info("setting output path");
        FileOutputFormat.setOutputPath(job1,new Path(args[3]+"inter"));
        logger.info("setting output finish");
        job1.waitForCompletion(true);
        System.exit(0);}
}