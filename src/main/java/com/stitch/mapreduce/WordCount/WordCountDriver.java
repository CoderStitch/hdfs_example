package com.stitch.mapreduce.WordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 输入输出路径需要根据自己电脑上实际的输入输出路径设置
        args = new String[]{"/ubuntu/input/wordcount", "/ubuntu/output/wordcount/out"};
        System.setProperty("hadoop.home.dir", "D:\\codeSoftware\\hadoop-2.10.1");
        System.setProperty("HADOOP_USER_NAME", "ubuntu");
        // 1 获取配置信息以及封装任务
        Configuration configuration = new Configuration();

        configuration.set("dfs.data.transfer.protection","integrity");
        configuration.set("mapreduce.framework.name", "yarn");
        configuration.set("fs.default.name", "hdfs://150.158.174.69:9000");
        configuration.set("mapreduce.app-submission.cross-platform", "true");
        configuration.set("yarn.resourcemanager.hostname","150.158.174.69");

        Job job = Job.getInstance(configuration);
        // 2 设置 jar 加载路径
        //job.setJarByClass(WordCountDriver.class);
        job.setJar("D:\\code\\hadoop\\example_hdfs\\target\\hadoop-1.0-SNAPSHOT.jar");
        // 3 设置 map 和 reduce 类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        // 4 设置 map 输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 5 设置最终输出 kv 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 如果不设置 InputFormat，它默认用的是 TextInputFormat.class
        job.setInputFormatClass(CombineTextInputFormat.class);
        //虚拟存储切片最大值设置 4m
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 7 提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);



    }
}
