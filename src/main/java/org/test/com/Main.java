package org.test.com;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;

public class Main {

    public static void main(String[] args){

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.master", "local")
                .getOrCreate();

        String path = Main.class.getClassLoader().getResource("test.json").getPath();
        Dataset<Row> df = spark.read().json(path);
        df.createOrReplaceTempView("test");
        df.show();

        Dataset<Row> tabl = spark.sql("SELECT l.numeric1, l.numeric2, l.nominal1, l.nominal2 FROM test LATERAL VIEW explode(array) AS l");
        tabl.show();
    }

}