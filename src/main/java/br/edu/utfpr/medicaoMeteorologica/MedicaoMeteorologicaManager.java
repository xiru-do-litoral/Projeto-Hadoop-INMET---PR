package br.edu.utfpr.medicaoMeteorologica;

import java.io.IOException;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MedicaoMeteorologicaManager extends Configured implements Tool {

	private static final String MEDICAO_TYPE = "medicaoType";

	private static final int DATE_ORDER = 1;

	private static final int ESTACAO_ORDER = 0;

	private static final String SEPARATOR = ";";
	
	public static String medicaoType;

	public static class MedicaoMeteorologicaMapper extends Mapper<Object, Text, MedicaoWritable, FloatWritable> {

		private static final String DATE_SEPARATOR = "/";
		

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			medicaoType = context.getConfiguration().get(MEDICAO_TYPE);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			final String[] values = value.toString().split(SEPARATOR);
			final MedicaoWritable medicao = getMedicao(values, medicaoType);
			//System.out.println("values: "+values.length+values[0]+" - "+values[1]+" - "+values[2]+" - "+values[3]);
			
			final String medicaoValue;
			
			if (MedicaoType.getOrder(medicaoType) >= values.length) {
				medicaoValue = format("0"); 
		    }else{
		    	medicaoValue = format(values[MedicaoType.getOrder(medicaoType)]);
		    }
			
			
			//System.out.println(medicao+" - "+medicaoValue);
			if (medicao != null && NumberUtils.isNumber(medicaoValue)) {
				context.write(medicao, new FloatWritable(Float.valueOf(medicaoValue)));
			}
		}

		private MedicaoWritable getMedicao(String[] values, String medicaoType) {
			MedicaoWritable medicaoWritable = null;

			final String date = format(values[DATE_ORDER]);

			if (isValidData(date)) {
				final String ano = date.split(DATE_SEPARATOR)[2];
				final String estacao = format(values[ESTACAO_ORDER]);

				medicaoWritable = new MedicaoWritable(ano, estacao);
			}

			return medicaoWritable;
		}

		private boolean isValidData(final String date) {
			return date.contains(DATE_SEPARATOR);
		}

		private String format(String value) {
			return value.trim();
		}
	}

	public static class MedicaoMeteorologicaReducer extends Reducer<MedicaoWritable, FloatWritable, MedicaoWritable, FloatWritable> {

		public void reduce(MedicaoWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
			float medicao = 0f;
			
			for (FloatWritable medicaoValue : values) {
					medicao = Math.max(medicao, medicaoValue.get());
			}
			context.write(key, new FloatWritable(medicao));
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 3) {
			System.err.println("MedicaoMeteorologicaManager requer os parametros: <input file> <output dir> <medicao type>");
			System.exit(2);
		}

		deleteOutputFileIfExists(args);

		final Configuration configuration = new Configuration();
		configuration.set(MEDICAO_TYPE, args[2]);

		@SuppressWarnings("deprecation")
		final Job job = new Job(configuration);

		job.setJarByClass(MedicaoMeteorologicaManager.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(MedicaoWritable.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setOutputKeyClass(MedicaoWritable.class);
		job.setOutputValueClass(FloatWritable.class);

		job.setMapperClass(MedicaoMeteorologicaMapper.class);
		job.setReducerClass(MedicaoMeteorologicaReducer.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		return 0;
	}

	private void deleteOutputFileIfExists(String[] args) throws IOException {
		final Path output = new Path(args[1]);
		FileSystem.get(output.toUri(), getConf()).delete(output, true);
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new MedicaoMeteorologicaManager(), args);
	}
}
