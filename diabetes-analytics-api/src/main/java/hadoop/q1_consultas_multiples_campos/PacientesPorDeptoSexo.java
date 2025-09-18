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

public class PacientesPorDeptoSexo {

    public static class PacientesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text outKey = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Omitir la cabecera del CSV
            if (key.get() == 0) {
                return;
            }

            String[] fields = value.toString().split(";");
            if (fields.length > 9) {
                String departamento = fields[1].trim();
                String sexo = fields[9].trim();

                // Clave compuesta: "DEPARTAMENTO;SEXO"
                outKey.set(departamento + ";" + sexo);
                context.write(outKey, one);
            }
        }
    }

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
        Job job = Job.getInstance(conf, "Pacientes por Departamento y Sexo");
        job.setJarByClass(PacientesPorDeptoSexo.class);
        job.setMapperClass(PacientesMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return job.waitForCompletion(true);
    }
}