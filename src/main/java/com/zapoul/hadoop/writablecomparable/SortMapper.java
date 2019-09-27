package com.zapoul.hadoop.writablecomparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created with IDEA
 * author:zapoul
 * Date:2019/9/27
 * Time:11:21
 */
public class SortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    private FlowBean flowBean = new FlowBean();
    private Text phone = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        phone.set(split[0]);
        flowBean.setUpFlow(Long.parseLong(split[1]));
        flowBean.setDownFlow(Long.parseLong(split[2]));
        flowBean.setSumFlow(Long.parseLong(split[3]));

        context.write(flowBean, phone);
    }
}
