package com.zapoul.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created with IDEA
 * author:zapoul
 * Date:2019/9/18
 * Time:14:48
 * LongWritable 偏移量
 * Text  文件的文字
 * Text  输出值key
 * IntWritable 输出值value
 */
public class WcMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    //对应java的string
    private Text word = new Text();
    //对应java的int
    private IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //拿到这一行数据
        String line = value.toString();

        //按照空格切分数据
        String[] words = line.split(" ");

        //遍历数组，把单词变成（word,1）的形式交给框架
        for (String word : words) {
            this.word.set(word);
            context.write(this.word, this.one);
        }
    }


}
