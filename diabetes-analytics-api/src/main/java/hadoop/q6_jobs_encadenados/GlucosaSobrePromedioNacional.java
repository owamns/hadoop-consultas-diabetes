package hadoop.q6_jobs_encadenados;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class GlucosaSobrePromedioNacional {

    // ================= JOB 1: CALCULAR PROMEDIO GLOBAL =================
    public static class GlobalAvgMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static final Text KEY_GLOBAL = new Text("GLOBAL");
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0) return;
            String[] fields = value.toString().split(";", -1);
            if (fields.length > 24) {
                try {
                    double glucosa = -1;
                    if (fields[19].toUpperCase().contains("GLUCOSA")) glucosa = Double.parseDouble(fields[20]);
                    else if (fields[23].toUpperCase().contains("GLUCOSA")) glucosa = Double.parseDouble(fields[24]);

                    if (glucosa > 0) {
                        context.write(KEY_GLOBAL, new Text(glucosa + ",1")); // Emitimos valor y contador
                    }
                } catch (NumberFormatException e) {
                    // Ignorar errores de parseo
                }
            }
        }
    }

    public static class GlobalAvgCombiner extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            long count = 0;
            for (Text val : values) {
                String[] parts = val.toString().split(",");
                sum += Double.parseDouble(parts[0]);
                count += Long.parseLong(parts[1]);
            }
            if (count > 0) {
                context.write(key, new Text(sum + "," + count));
            }
        }
    }

    public static class GlobalAvgReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            long count = 0;
            for (Text val : values) {
                String[] parts = val.toString().split(",");
                sum += Double.parseDouble(parts[0]);
                count += Long.parseLong(parts[1]);
            }
            if (count > 0) {
                context.write(new Text(String.valueOf(sum / count)), null);
            }
        }
    }

    // ================= JOB 2: FILTRAR DEPARTAMENTOS =================
    public static class DeptoAvgMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0) return;
            String[] fields = value.toString().split(";", -1);
            if (fields.length > 24) {
                try {
                    double glucosa = -1;
                    String depto = fields[1].trim();
                    if (fields[19].toUpperCase().contains("GLUCOSA")) glucosa = Double.parseDouble(fields[20]);
                    else if (fields[23].toUpperCase().contains("GLUCOSA")) glucosa = Double.parseDouble(fields[24]);

                    if (glucosa > 0 && !depto.isEmpty()) {
                        context.write(new Text(depto), new Text(glucosa + ",1"));
                    }
                } catch (NumberFormatException e) {
                    // Ignorar errores de parseo
                }
            }
        }
    }

    public static class DeptoFilterReducer extends Reducer<Text, Text, Text, Text> {
        private double globalAvg = 0.0;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Leer el promedio global de la configuración
            globalAvg = Double.parseDouble(context.getConfiguration().get("global.avg.glucose"));
        }
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            long count = 0;
            for (Text val : values) {
                String[] parts = val.toString().split(",");
                sum += Double.parseDouble(parts[0]);
                count += Long.parseLong(parts[1]);
            }
            if (count > 0) {
                double deptoAvg = sum / count;
                if (deptoAvg > globalAvg) {
                    String output = String.format("Promedio: %.2f (Superior al nacional de %.2f)", deptoAvg, globalAvg);
                    context.write(key, new Text(output));
                }
            }
        }
    }

    public static boolean runJob(String inputPathStr, String outputPathStr) throws Exception {
        Configuration conf = new Configuration();
        Path inputPath = new Path(inputPathStr);
        Path globalAvgPath = new Path(outputPathStr + "_temp_global_avg");
        Path finalOutputPath = new Path(outputPathStr);

        // Job 1: Calculate global average
        Job job1 = Job.getInstance(conf, "Paso 1: Calcular Promedio Global de Glucosa");
        job1.setJarByClass(GlucosaSobrePromedioNacional.class);
        job1.setMapperClass(GlobalAvgMapper.class);
        job1.setCombinerClass(GlobalAvgCombiner.class);
        job1.setReducerClass(GlobalAvgReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, globalAvgPath);
        if (!job1.waitForCompletion(true)) {
            return false;
        }

        // Read global average
        FileSystem fs = FileSystem.get(conf);
        Path resultFile = new Path(globalAvgPath, "part-r-00000");
        String globalAvgStr;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(resultFile)))) {
            globalAvgStr = br.readLine();
            if (globalAvgStr != null) globalAvgStr = globalAvgStr.trim();
        }
        if (globalAvgStr == null || globalAvgStr.isEmpty()) {
            fs.delete(globalAvgPath, true);
            return false;
        }

        // Job 2: Filter departments
        conf.set("global.avg.glucose", globalAvgStr);
        Job job2 = Job.getInstance(conf, "Paso 2: Filtrar Departamentos");
        job2.setJarByClass(GlucosaSobrePromedioNacional.class);
        job2.setMapperClass(DeptoAvgMapper.class);
        job2.setReducerClass(DeptoFilterReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, inputPath);
        FileOutputFormat.setOutputPath(job2, finalOutputPath);
        boolean success = job2.waitForCompletion(true);

        // Cleanup
        fs.delete(globalAvgPath, true);
        return success;
    }
}