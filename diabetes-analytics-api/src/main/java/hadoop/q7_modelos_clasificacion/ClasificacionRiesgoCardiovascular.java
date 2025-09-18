package hadoop.q7_modelos_clasificacion;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ClasificacionRiesgoCardiovascular {

    public static class RiesgoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        // Modelo de clasificación basado en reglas
        private String getRiskCategory(double glucosa, double colesterol) {
            boolean glucosaAlta = glucosa >= 126;
            boolean glucosaLimite = glucosa >= 100 && glucosa < 126;
            boolean colesterolAlto = colesterol >= 240;
            boolean colesterolLimite = colesterol >= 200 && colesterol < 240;

            if (glucosaAlta && colesterolAlto) return "RIESGO_CRITICO";
            if (glucosaAlta || colesterolAlto) return "RIESGO_ALTO";
            if (glucosaLimite || colesterolLimite) return "RIESGO_MODERADO";
            return "RIESGO_BAJO";
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0) return;
            String[] fields = value.toString().split(";", -1);
            if (fields.length > 24) {
                try {
                    double glucosa = -1, colesterol = -1;
                    int edad = Integer.parseInt(fields[8]);

                    if (fields[19].toUpperCase().contains("GLUCOSA")) glucosa = Double.parseDouble(fields[20]);
                    else if (fields[19].toUpperCase().contains("COLESTEROL")) colesterol = Double.parseDouble(fields[20]);

                    if (fields[23].toUpperCase().contains("GLUCOSA")) glucosa = Double.parseDouble(fields[24]);
                    else if (fields[23].toUpperCase().contains("COLESTEROL")) colesterol = Double.parseDouble(fields[24]);

                    if (glucosa > 0 && colesterol > 0) {
                        String categoria = getRiskCategory(glucosa, colesterol);
                        context.write(new Text(categoria), new IntWritable(edad));
                    }
                } catch (Exception e) {}
            }
        }
    }

    public static class AvgReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            long count = 0;
            for(IntWritable val : values) {
                sum += val.get();
                count++;
            }
            if (count > 0) {
                context.write(key, new DoubleWritable((double)sum / count));
            }
        }
    }

    public static boolean runJob(String inputPath, String outputPath) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Clasificación de Riesgo y Edad Promedio");
        job.setJarByClass(ClasificacionRiesgoCardiovascular.class);
        job.setMapperClass(RiesgoMapper.class);
        job.setReducerClass(AvgReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return job.waitForCompletion(true);
    }
}