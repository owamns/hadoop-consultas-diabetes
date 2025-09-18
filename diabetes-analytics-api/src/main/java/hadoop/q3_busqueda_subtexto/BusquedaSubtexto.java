package hadoop.q3_busqueda_subtexto;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class BusquedaSubtexto {

    public static class SearchMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        private String searchTerm;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Obtener el término de búsqueda de la configuración del job
            searchTerm = context.getConfiguration().get("searchTerm").toUpperCase();
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0) return;

            String[] fields = value.toString().split(";");
            if (fields.length > 19) {
                String diagnostico = fields[13].toUpperCase();
                String procedimiento1 = fields[19].toUpperCase();

                if (diagnostico.contains(searchTerm) || procedimiento1.contains(searchTerm)) {
                    context.write(value, NullWritable.get());
                }
            }
        }
    }

    public static boolean runJob(String inputPath, String outputPath, String searchTerm) throws Exception {
        Configuration conf = new Configuration();
        conf.set("searchTerm", searchTerm);
        Job job = Job.getInstance(conf, "Busqueda de Subtexto");
        job.setJarByClass(BusquedaSubtexto.class);
        job.setMapperClass(SearchMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return job.waitForCompletion(true);
    }

}