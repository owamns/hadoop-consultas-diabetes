package hadoop.q5_valores_extremos;

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

public class MinMaxColesterolPorDepto {

    public static class MinMaxMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text departamento = new Text();
        private DoubleWritable resultado = new DoubleWritable();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0) return;

            String[] fields = value.toString().split(";");
            if (fields.length > 20) {
                try {
                    // Asumimos que RESULTADO_1 es colesterol
                    double res1 = Double.parseDouble(fields[20].trim());
                    String depto = fields[1].trim();

                    departamento.set(depto);
                    resultado.set(res1);
                    context.write(departamento, resultado);
                } catch (NumberFormatException e) {
                    // Ignorar
                }
            }
        }
    }

    public static class MinMaxReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;

            for (DoubleWritable val : values) {
                double v = val.get();
                if (v < min) {
                    min = v;
                }
                if (v > max) {
                    max = v;
                }
            }
            result.set("Min: " + min + ", Max: " + max);
            context.write(key, result);
        }
    }

    public static boolean runJob(String inputPath, String outputPath) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Min-Max Colesterol por Departamento");
        job.setJarByClass(MinMaxColesterolPorDepto.class);
        job.setMapperClass(MinMaxMapper.class);
        job.setReducerClass(MinMaxReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return job.waitForCompletion(true);
    }

}