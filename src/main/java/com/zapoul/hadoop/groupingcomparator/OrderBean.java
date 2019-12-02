package com.zapoul.hadoop.groupingcomparator;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IDEA
 * author:zapoul
 * Date:2019/9/27
 * Time:15:41
 */
public class OrderBean implements WritableComparable<OrderBean> {

    private String orderId;

    private String productId;

    private double price;

    @Override
    public String toString() {
        return
                orderId + '\t' +
                        productId + '\t' +
                        price;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public int compareTo(OrderBean o) {
        //先按订单排序，后按价格排序
        int i = this.orderId.compareTo(o.orderId);
        if (i == 0) {
            return Double.compare(o.price, this.price);
        } else {
            return i;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);
        dataOutput.writeUTF(productId);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.orderId = dataInput.readUTF();
        this.productId = dataInput.readUTF();
        this.price = dataInput.readDouble();
    }
}
