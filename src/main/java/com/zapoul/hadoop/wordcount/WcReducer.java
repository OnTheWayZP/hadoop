package com.zapoul.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created with IDEA
 * author:zapoul
 * Date:2019/9/18
 * Time:14:49
 * Text  map处理后返回的key
 * IntWritable  map处理后返回的key对应value的分组
 * Text  reducer处理后返回的key
 * IntWritable  reducer处理后返回的value
 */
public class WcReducer extends Reducer<Text, IntWritable,Text, IntWritable> {

    private IntWritable total = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //计算同一个key的数量
        int sum = 0;
        for (IntWritable value : values) {
            sum+=value.get();
        }
        total.set(sum);
        context.write(key,total);
    }
}
