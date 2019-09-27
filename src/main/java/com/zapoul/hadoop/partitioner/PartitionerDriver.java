package com.zapoul.hadoop.partitioner;

import com.zapoul.hadoop.flow.FlowBean;
import com.zapoul.hadoop.flow.FlowMapper;
import com.zapoul.hadoop.flow.FlowReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 实现自定义分区
 * Created with IDEA
 * author:zapoul
 * Date:2019/9/26
 * Time:20:51
 */
public class PartitionerDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取一个job实例
        Job job = Job.getInstance(new Configuration());

        //2.设置我们的类路径
        job.setJarByClass(PartitionerDriver.class);

        //3.设置mapper和reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setNumReduceTasks(5);
        job.setPartitionerClass(MyPartitioner.class);

        //4.设置Mapper和Reducer输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //5.设置输入输出数据
        FileInputFormat.setInputPaths(job, new Path("D:\\input\\phone"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\output\\out1"));

        //6. 提交我们的job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
