package hadoop.q1_consultas_multiples_campos;

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

public class ProcedimientosPorAreaServicio {
    public static class ProcedimientoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text outKey = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0) return;

            String[] fields = value.toString().split(";");
            if (fields.length > 15) {
                String area = fields[14].trim();
                String servicio = fields[15].trim();

                outKey.set(area + ";" + servicio);
                // Cada registro con un procedimiento válido cuenta como uno.
                // Si hay dos procedimientos por fila, los contamos ambos.
                if (!fields[19].trim().isEmpty()) {
                    context.write(outKey, one);
                }
                if (fields.length > 23 && !fields[23].trim().isEmpty()) {
                    context.write(outKey, one);
                }
            }
        }
    }

    // Reutilizamos el SumReducer de la consulta 1.1
    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static boolean runJob(String inputPath, String outputPath) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Procedimientos por Area y Servicio");
        job.setJarByClass(ProcedimientosPorAreaServicio.class);
        job.setMapperClass(ProcedimientoMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return job.waitForCompletion(true);
    }

}
