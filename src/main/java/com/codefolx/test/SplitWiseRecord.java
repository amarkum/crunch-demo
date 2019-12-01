package com.codefolx.test;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SplitWiseRecord  {

    String date;
    String description;
    String category;
    double cost;
    String currency;
    double roomMate1;
    double roomMate2;
    double roomMate3;

    public SplitWiseRecord(String date, String description, String category, double cost, String currency, double roomMate1, double roomMate2, double roomMate3) {
        this.date = date;
        this.description = description;
        this.category = category;
        this.cost = cost;
        this.currency = currency;
        this.roomMate1 = roomMate1;
        this.roomMate2 = roomMate2;
        this.roomMate3 = roomMate3;
    }

}
