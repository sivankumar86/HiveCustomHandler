package com.ask.hive.hbase;


import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.hbase.client.Result;

/**
 * TODO: Enter JavaDoc
 *
 * @since JDK 1.5
 */
public class ListTableInputFormat extends TextTableInputFormat {

    public static final String VALUE_SEPARATOR_KEY = "map.input.value.separator";
    public static final String DEFAULT_VALUE_SEPARATOR = " ";

    private String valueSeparator;


    @Override
    public void configure(JobConf job) {
        super.configure(job);
        valueSeparator = job.get(VALUE_SEPARATOR_KEY);
        if (valueSeparator == null)
            valueSeparator = DEFAULT_VALUE_SEPARATOR;
    }

    public String getValueSeparator() {
        return valueSeparator;
    }


    public String formatRowResult(Result row) {
        StringBuilder values = new StringBuilder("");
        if (!row.isEmpty()) {
            values.append(row.toString());
        }
        return values.toString();
    }

}
