package hadoop.q7_modelos_clasificacion;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PrediccionReingresoSimple {

    public static class PredictionMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        // "Pesos" del modelo que se leerán de la configuración
        private double weightAge, weightGlucose, threshold;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            weightAge = conf.getDouble("model.weight.age", 0.02); // Valor por defecto 0.02
            weightGlucose = conf.getDouble("model.weight.glucose", 0.01); // Valor por defecto 0.01
            threshold = conf.getDouble("model.threshold", 3.0); // Valor por defecto 3.0
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0) return;
            String[] fields = value.toString().split(";", -1);
            if (fields.length > 24) {
                try {
                    double glucosa = -1;
                    int edad = Integer.parseInt(fields[8]);

                    if (fields[19].toUpperCase().contains("GLUCOSA")) glucosa = Double.parseDouble(fields[20]);
                    else if (fields[23].toUpperCase().contains("GLUCOSA")) glucosa = Double.parseDouble(fields[24]);

                    if (glucosa > 0) {
                        // Aplicar el modelo de predicción
                        double score = (weightAge * edad) + (weightGlucose * glucosa);
                        String prediction = (score > threshold) ? "ALTA_PROBABILIDAD_REINGRESO" : "BAJA_PROBABILIDAD_REINGRESO";

                        // Contar cuántos pacientes hay en cada categoría
                        context.write(new Text(prediction), new IntWritable(1));
                    }
                } catch (Exception e) {}
            }
        }
    }

    public static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static boolean runJob(String inputPath, String outputPath) throws Exception {
        Configuration conf = new Configuration();
        // Set default hyperparameters
        conf.setDouble("model.weight.age", 0.03);
        conf.setDouble("model.weight.glucose", 0.015);
        conf.setDouble("model.threshold", 4.0);
        Job job = Job.getInstance(conf, "Predicción de Reingreso Simple");
        job.setJarByClass(PrediccionReingresoSimple.class);
        job.setMapperClass(PredictionMapper.class);
        job.setCombinerClass(CountReducer.class);
        job.setReducerClass(CountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return job.waitForCompletion(true);
    }
}