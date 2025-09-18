package hadoop.q2_estadisticas_descriptivas;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class EstadisticasColesterol {

    // Mapper emite el valor del resultado del colesterol
    public static class StatsMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final static Text keyText = new Text("COLESTEROL_RESULTADO_1");
        private DoubleWritable outValue = new DoubleWritable();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0) return;

            String[] fields = value.toString().split(";");
            if (fields.length > 20) {
                try {
                    // Procedimiento 1 es Colesterol
                    if(fields[19].toUpperCase().contains("COLESTEROL")){
                        double resultado = Double.parseDouble(fields[20].trim());
                        outValue.set(resultado);
                        context.write(keyText, outValue);
                    }
                    // Procedimiento 2 es Colesterol
                    else if (fields.length > 23 && fields[23].toUpperCase().contains("COLESTEROL")){
                        double resultado = Double.parseDouble(fields[24].trim());
                        outValue.set(resultado);
                        context.write(keyText, outValue);
                    }

                } catch (NumberFormatException e) {
                    // Ignorar
                }
            }
        }
    }

    // Reducer calcula promedio, mediana y desviación estándar
    public static class StatsReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            ArrayList<Double> listaValores = new ArrayList<>();
            double sum = 0;
            double sumSq = 0;
            long count = 0;

            for (DoubleWritable val : values) {
                double v = val.get();
                listaValores.add(v);
                sum += v;
                sumSq += v * v;
                count++;
            }

            // Calcular Promedio
            double mean = sum / count;

            // Calcular Desviación Estándar
            double stdDev = Math.sqrt((sumSq / count) - (mean * mean));

            // Calcular Mediana
            Collections.sort(listaValores);
            double median;
            if (count % 2 == 0) {
                median = (listaValores.get((int) (count / 2) - 1) + listaValores.get((int) (count / 2))) / 2.0;
            } else {
                median = listaValores.get((int) (count / 2));
            }

            String output = String.format("Promedio: %.2f, Mediana: %.2f, Desv. Estandar: %.2f, Registros: %d", mean, median, stdDev, count);
            result.set(output);
            context.write(key, result);
        }
    }

    public static boolean runJob(String inputPath, String outputPath) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Estadisticas de Colesterol");
        job.setJarByClass(EstadisticasColesterol.class);
        job.setMapperClass(StatsMapper.class);
        job.setReducerClass(StatsReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return job.waitForCompletion(true);
    }

}