package hadoop.q1_consultas_multiples_campos;

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

public class EdadPromedioPorDiagnostico {

    public static class EdadMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text diagnostico = new Text();
        private IntWritable edad = new IntWritable();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0) return;

            String[] fields = value.toString().split(";");
            if (fields.length > 13) {
                try {
                    String diag = fields[13].trim();
                    int edadPaciente = Integer.parseInt(fields[8].trim());

                    diagnostico.set(diag);
                    edad.set(edadPaciente);
                    context.write(diagnostico, edad);
                } catch (NumberFormatException e) {
                    // Ignorar registros con edad inválida
                }
            }
        }
    }

    public static class AvgReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            for (IntWritable val : values) {
                sum += val.get();
                count++;
            }
            double avg = (double) sum / count;
            result.set(avg);
            context.write(key, result);
        }
    }

    public static boolean runJob(String inputPath, String outputPath) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Edad Promedio por Diagnostico");
        job.setJarByClass(EdadPromedioPorDiagnostico.class);
        job.setMapperClass(EdadMapper.class);
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
