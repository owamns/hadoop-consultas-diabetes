package hadoop.q6_jobs_encadenados;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class NormalizacionMinMaxColesterol {

    public static class MinMaxMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0) return;
            String[] fields = value.toString().split(";", -1);
            if (fields.length > 20) {
                try {
                    String provincia = fields[2].trim();
                    double colesterol = -1;
                    if (fields[19].toUpperCase().contains("COLESTEROL")) colesterol = Double.parseDouble(fields[20]);
                    else if (fields.length > 23 && fields[23].toUpperCase().contains("COLESTEROL")) colesterol = Double.parseDouble(fields[24]);

                    if (colesterol > 0 && !provincia.isEmpty()) {
                        context.write(new Text(provincia), new DoubleWritable(colesterol));
                    }
                } catch (Exception e) {}
            }
        }
    }

    public static class MinMaxReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;
            for (DoubleWritable val : values) {
                min = Math.min(min, val.get());
                max = Math.max(max, val.get());
            }
            context.write(key, new Text(min + ";" + max));
        }
    }

    public static class NormalizationMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        private Map<String, String> provinciaMinMaxMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI file : cacheFiles) {
                    Path localPath = new Path(file.getPath());
                    try (BufferedReader reader = new BufferedReader(new FileReader(localPath.getName()))) {
                        String line;
                        while ((line = reader.readLine()) != null) {
                            String[] parts = line.split("\t");
                            if (parts.length == 2) {
                                provinciaMinMaxMap.put(parts[0], parts[1]);
                            }
                        }
                    }
                }
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0) {
                context.write(new Text(value.toString() + ";COLESTEROL_NORMALIZADO"), NullWritable.get());
                return;
            }
            String[] fields = value.toString().split(";", -1);
            String originalLine = value.toString();
            String provincia = fields[2].trim();
            String minMaxStr = provinciaMinMaxMap.get(provincia);

            if (minMaxStr != null) {
                try {
                    double colesterol = -1;
                    if (fields[19].toUpperCase().contains("COLESTEROL")) colesterol = Double.parseDouble(fields[20]);
                    else if (fields.length > 23 && fields[23].toUpperCase().contains("COLESTEROL")) colesterol = Double.parseDouble(fields[24]);

                    if (colesterol > 0) {
                        String[] minMaxParts = minMaxStr.split(";");
                        double min = Double.parseDouble(minMaxParts[0]);
                        double max = Double.parseDouble(minMaxParts[1]);

                        double normalizedValue = 0.0;
                        if (max - min != 0) {
                            normalizedValue = (colesterol - min) / (max - min);
                        }

                        context.write(new Text(originalLine + ";" + String.format("%.4f", normalizedValue)), NullWritable.get());
                        return;
                    }
                } catch (Exception e) {}
            }
            context.write(new Text(originalLine + ";"), NullWritable.get());
        }
    }

    public static boolean runJob(String inputPathStr, String outputPathStr) throws Exception {
        Configuration conf = new Configuration();
        Path inputPath = new Path(inputPathStr);
        Path minMaxPath = new Path(outputPathStr + "_temp_minmax");
        Path finalOutputPath = new Path(outputPathStr);

        Job job1 = Job.getInstance(conf, "Paso 1: Calcular Min-Max de Colesterol por Provincia");
        job1.setJarByClass(NormalizacionMinMaxColesterol.class);
        job1.setMapperClass(MinMaxMapper.class);
        job1.setReducerClass(MinMaxReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(DoubleWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, minMaxPath);

        if (!job1.waitForCompletion(true)) {
            return false;
        }

        Job job2 = Job.getInstance(conf, "Paso 2: Aplicar Normalización Min-Max");
        job2.setJarByClass(NormalizacionMinMaxColesterol.class);
        job2.addCacheFile(new URI(minMaxPath.toString() + "/part-r-00000"));
        job2.setMapperClass(NormalizationMapper.class);
        job2.setNumReduceTasks(0);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job2, inputPath);
        FileOutputFormat.setOutputPath(job2, finalOutputPath);

        boolean success = job2.waitForCompletion(true);

        FileSystem fs = FileSystem.get(conf);
        fs.delete(minMaxPath, true);
        return success;
    }
}
