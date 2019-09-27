package com.zapoul.hadoop.writablecomparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created with IDEA
 * author:zapoul
 * Date:2019/9/27
 * Time:11:22
 */
public class SortReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text text : values) {
            context.write(text, key);
        }
    }
}
