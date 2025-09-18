package hadoop.q4_busqueda_rango_fechas;

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

public class BusquedaPorFechas {

    public static class DateRangeMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        private int startDate;
        private int endDate;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            startDate = Integer.parseInt(conf.get("startDate"));
            endDate = Integer.parseInt(conf.get("endDate"));
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0) return;

            String[] fields = value.toString().split(";");
            if (fields.length > 17) {
                try {
                    int fechaMuestra = Integer.parseInt(fields[17].trim());
                    if (fechaMuestra >= startDate && fechaMuestra <= endDate) {
                        context.write(value, NullWritable.get());
                    }
                } catch (NumberFormatException e) {
                    // Ignorar registro si la fecha no es un número válido
                }
            }
        }
    }

    public static boolean runJob(String inputPath, String outputPath, String startDate, String endDate) throws Exception {
        Configuration conf = new Configuration();
        conf.set("startDate", startDate);
        conf.set("endDate", endDate);
        Job job = Job.getInstance(conf, "Busqueda por Rango de Fechas");
        job.setJarByClass(BusquedaPorFechas.class);
        job.setMapperClass(DateRangeMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return job.waitForCompletion(true);
    }

}