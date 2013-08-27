package com.ask.hive.hbase;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableInputFormat;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * TODO: Enter JavaDoc
 *
 * @since JDK 1.5
 */
public abstract class TextTableInputFormat
    implements InputFormat<Text, Text> {

    public static final String TABLE_KEY = "map.input.table";
    public static final String COLUMNS_KEY = "map.input.columns";
    public static final String HAS_TIMESTAMP_KEY = "map.input.timestamp";
    public static final String IS_BINARY_KEY = "map.input.binary";

    protected TableInputFormat inputFormat;
    private boolean hasTimestamp;
    private boolean isBinary;

    public TextTableInputFormat() {
        inputFormat = new TableInputFormat();
    }

    public void configure(JobConf job) {
        FileInputFormat.setInputPaths(job, "sem_test");
        job.set(TableInputFormat.COLUMN_LIST, "tf");
        inputFormat.configure(job);
        hasTimestamp = argToBoolean(job.get(HAS_TIMESTAMP_KEY));
        isBinary = argToBoolean(job.get(IS_BINARY_KEY));
    }

    public boolean hasTimestamp() { return hasTimestamp; }
    public boolean isBinary() { return isBinary; }

    protected String encodeColumnName(byte[] key) {
        return isBinary() ? Base64.encodeBytes(key) : new String(key);
    }
    protected String encodeValue(byte[] value) {
        return isBinary() ? Base64.encodeBytes(value) : new String(value);
    }

    protected boolean argToBoolean(String arg) {
        if (arg == null) return false;
        return arg.equals("true")
            || arg.equals("yes")
            || arg.equals("on")
            || arg.equals("1");
    }

    public void validateInput(JobConf job) throws IOException {
        inputFormat.validateInput(job);
    }

    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        return inputFormat.getSplits(job, numSplits);
    }

    public RecordReader<Text, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
        return new TextTableRecordReader(inputFormat.getRecordReader(split, job, reporter));
    }

    public abstract String formatRowResult(Result row);

    protected class TextTableRecordReader implements RecordReader<Text, Text> {
        private RecordReader<ImmutableBytesWritable, Result> tableRecordReader;

        public TextTableRecordReader(RecordReader<ImmutableBytesWritable, Result> reader) {
            tableRecordReader = reader;
        }

        public void close() throws IOException {
            tableRecordReader.close();
        }

        public Text createKey() {
            return new Text("");
        }

        public Text createValue() {
            return new Text("");
        }

        public long getPos() throws IOException {
            return tableRecordReader.getPos();
        }

        public float getProgress() throws IOException {
            return tableRecordReader.getProgress();
        }

        public boolean next(Text key, Text value) throws IOException {
            Result row = new Result();
            boolean hasNext = tableRecordReader.next(new ImmutableBytesWritable(key.getBytes()), row);
            if (hasNext) {
                key.set(row.getRow());
                value.set(formatRowResult(row));
            }
            return hasNext;
        }
    }

}