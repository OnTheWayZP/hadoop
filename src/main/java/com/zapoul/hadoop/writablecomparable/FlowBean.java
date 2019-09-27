package com.zapoul.hadoop.writablecomparable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IDEA
 * author:zapoul
 * Date:2019/9/24
 * Time:9:04
 */
public class FlowBean implements Writable, WritableComparable<FlowBean> {

    private long upFlow;

    private long downFlow;

    private long sumFlow;

    public FlowBean() {
    }

    public void set(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    /**
     * 序列化
     *
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(sumFlow);
    }

    /**
     * 反序列化
     *
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        downFlow = dataInput.readLong();
        upFlow = dataInput.readLong();
        sumFlow = dataInput.readLong();
    }

    @Override
    public String toString() {
        return
                "upFlow=" + upFlow + "\t" +
                        "downFlow=" + downFlow + "\t" +
                        "sumFlow=" + sumFlow + "\t";
    }

    @Override
    public int compareTo(FlowBean o) {
        return Long.compare(o.sumFlow, this.sumFlow);
    }
}
