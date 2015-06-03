import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.TableName;


public class KMeans {

	public static int cons;
	public static long ItrCount = 0;
	public static boolean converge = false;

	public HbaseTest(int x){
		cons = x;
	}

	public enum Kmeans{
		count
	}



	public static class map1 
	extends Mapper<Object, Text, Text, Text>{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();

			context.write(new Text("a"),new Text(line));
		}
	}

	public static class reduce1 extends Reducer<Text,Text,Text,Text> {
		int Rowkey = 1;
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			for (Text val : values){
				context.write(new Text("row"+(Rowkey++)), new Text(val));
			}
		}
	}


	public static class map2 extends Mapper<Object, Text,Text, Text>{

		public void map(Object key, Text data, Context context) throws IOException, InterruptedException{
			String[] abc = data.toString().split("\t");
			context.write(new Text(abc[0]), new Text(abc[1]+" "+abc[2]+" "+abc[3]+" "+abc[4]+" "+abc[5]+" "+abc[6]+" "+abc[7]+" "+abc[8]+" "+abc[9]+" "+abc[10]));
		}
	}

	public static class reduce2 extends TableReducer<Text, Text, ImmutableBytesWritable>  {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			Put put = new Put(Bytes.toBytes(key.toString()));
			for (Text val : values) {
				String[] a = val.toString().split(" ");

				put.add(Bytes.toBytes("Area"), Bytes.toBytes("X2"), Bytes.toBytes(a[1]));
				put.add(Bytes.toBytes("Area"), Bytes.toBytes("X3"), Bytes.toBytes(a[2]));
				put.add(Bytes.toBytes("Area"), Bytes.toBytes("X4"), Bytes.toBytes(a[3]));
				put.add(Bytes.toBytes("Area"), Bytes.toBytes("X7"), Bytes.toBytes(a[6]));
				put.add(Bytes.toBytes("Area"), Bytes.toBytes("X8"), Bytes.toBytes(a[7]));
				put.add(Bytes.toBytes("Property"), Bytes.toBytes("X1"), Bytes.toBytes(a[0]));
				put.add(Bytes.toBytes("Property"), Bytes.toBytes("X5"), Bytes.toBytes(a[4]));
				put.add(Bytes.toBytes("Property"), Bytes.toBytes("X6"), Bytes.toBytes(a[5]));
				put.add(Bytes.toBytes("Property"), Bytes.toBytes("Y1"), Bytes.toBytes(a[8]));
				put.add(Bytes.toBytes("Property"), Bytes.toBytes("Y2"), Bytes.toBytes(a[9]));

				context.write(null, put);
			}	    	
		}

	}


	public static class map3 extends TableMapper<Text, Text>  {

		public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {

			String rowkey = Bytes.toString(row.get());		

			String val0 = new String(value.getValue(Bytes.toBytes("Area"), Bytes.toBytes("X2")));
			String val1 = new String(value.getValue(Bytes.toBytes("Area"), Bytes.toBytes("X3")));
			String val2 = new String(value.getValue(Bytes.toBytes("Area"), Bytes.toBytes("X4")));
			String val3 = new String(value.getValue(Bytes.toBytes("Area"), Bytes.toBytes("X7")));
			String val4 = new String(value.getValue(Bytes.toBytes("Area"), Bytes.toBytes("X8")));
			String val5 = new String(value.getValue(Bytes.toBytes("Property"), Bytes.toBytes("X1")));
			String val6 = new String(value.getValue(Bytes.toBytes("Property"), Bytes.toBytes("X5")));
			String val7 = new String(value.getValue(Bytes.toBytes("Property"), Bytes.toBytes("X6")));
			String val8 = new String(value.getValue(Bytes.toBytes("Property"), Bytes.toBytes("Y1")));
			String val9 = new String(value.getValue(Bytes.toBytes("Property"), Bytes.toBytes("Y2")));

			String data = (val0+" "+val1+" "+val2+" "+val3+" "+val4+" "+val5+" "+val6+" "+val7+" "+val8+" "+val9); 
			context.write(new Text(rowkey), new Text(data));
		}
	}


	public static class reduce3 extends TableReducer<Text, Text, ImmutableBytesWritable>  {
		private static int k = cons;
		private String data = "";
		private int count=0;
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			Put put = new Put(Bytes.toBytes("row"));
			if(count<k){
				for (Text val : values) {
					data += val.toString()+",";
					
					put.add(Bytes.toBytes("Centroids"), Bytes.toBytes("centers"), Bytes.toBytes(data));

					count++;
				}

				context.write(null, put);

			}
		}

	}	


	public static class map4 extends TableMapper<IntWritable, Text>  {

		private static HashMap<Integer, String[]> hmap = new HashMap<Integer, String[]>();

		private static byte[] input = Bytes.toBytes("input");
		private String[] centers;	

		@Override
		protected void setup(Context context) throws IOException,
		InterruptedException {

			Configuration con = HBaseConfiguration.create();
			@SuppressWarnings("resource")
			HTable table = new HTable(con, "center");
			Get g = new Get(Bytes.toBytes("row"));
			Result result = table.get(g);

			String c = new String(result.getValue(Bytes.toBytes("Centroids"), Bytes.toBytes("centers"))); 
			centers = c.split(",");

			for(int i=0; i<centers.length;i++){
				String[] abc = centers[i].split(" ");
				hmap.put(i,(abc));
			}
		}


		public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {

			int point = 0; 
			double min = Double.MAX_VALUE;

			TableSplit currentSplit = (TableSplit)context.getInputSplit();
			byte[] tableName = currentSplit.getTableName(); 


			if(Arrays.equals(tableName, input)) {

				String val0 = new String(value.getValue(Bytes.toBytes("Area"), Bytes.toBytes("X2")));
				String val1 = new String(value.getValue(Bytes.toBytes("Area"), Bytes.toBytes("X3")));
				String val2 = new String(value.getValue(Bytes.toBytes("Area"), Bytes.toBytes("X4")));
				String val3 = new String(value.getValue(Bytes.toBytes("Area"), Bytes.toBytes("X7")));
				String val4 = new String(value.getValue(Bytes.toBytes("Area"), Bytes.toBytes("X8")));
				String val5 = new String(value.getValue(Bytes.toBytes("Property"), Bytes.toBytes("X1")));
				String val6 = new String(value.getValue(Bytes.toBytes("Property"), Bytes.toBytes("X5")));
				String val7 = new String(value.getValue(Bytes.toBytes("Property"), Bytes.toBytes("X6")));
				String val8 = new String(value.getValue(Bytes.toBytes("Property"), Bytes.toBytes("Y1")));
				String val9 = new String(value.getValue(Bytes.toBytes("Property"), Bytes.toBytes("Y2")));

				String val = (val0+" "+val1+" "+val2+" "+val3+" "+val4+" "+val5+" "+val6+" "+val7+" "+val8+" "+val9);
				String[] data = val.split(" "); 

				double[] datapoint = new double[data.length];
				for (int i = 0; i < datapoint.length; i++) {
					datapoint[i] = Double.parseDouble(data[i]);
				}


				for(int i = 0; i<hmap.size();i++){
					double result = 0.0;
					double[] dd = new double[10];
					String[] a= hmap.get(i);

					for(int j=0;j<10;j++)
						dd[j]= Double.parseDouble(a[j]);

					for(int z = 0;z<10;z++)
						result += Math.pow((dd[z]-datapoint[z]),2); 


					if(result<min){
						min = result;
						point = i;
					}	
				}

				context.write(new IntWritable(point),new Text(val));
			}

		} 

	}



	public static class reduce4 extends TableReducer<IntWritable, Text, ImmutableBytesWritable>  {
		private static int k= cons;
		String centerStr = "";
		int count = 0;
		String[] abc;
		double SumDiff=0.0;

		private static double[] convertToDouble(String[] x){
			double[] y = new double[10];
			for (int j = 0; j < x.length; j++)
				y[j] = Double.parseDouble(x[j]);	
			return y;
		}
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			double[] newCen = new double[10];
			int div = 0;

			for(Text val : values){
				String xyz = val.toString();
				String[] data = xyz.split(" ");
				double[] datapoint = new double[data.length];

				for (int i = 0; i < datapoint.length; i++)
					datapoint[i] = Double.parseDouble(data[i]);

				for (int j = 0; j < datapoint.length; j++)
					newCen[j] = newCen[j] + datapoint[j];
				div++;
			}

			for (int j = 0; j < 10; j++)
				newCen[j] = newCen[j]/div;


			if(count<k){
				for(int i=0;i<10;i++)
					centerStr = centerStr+newCen[i]+" ";
				centerStr = centerStr+",";
				count++;
			}

			if(count==k){
				String newStr = "";
				newStr = centerStr;
				double[] newCenterDouble = new double[10];
				double[] oldCenterDouble= new double[10];
				Configuration con = HBaseConfiguration.create();
				@SuppressWarnings("resource")
				HTable table = new HTable(con, "center");
				Get g = new Get(Bytes.toBytes("row"));
				Result result = table.get(g);

				String c = new String(result.getValue(Bytes.toBytes("Centroids"), Bytes.toBytes("centers"))); 

				String[] oldCenter = c.split(",");
				String[] newCenter = newStr.split(",");

				for(int x=0; x<oldCenter.length;x++){
					String[]  xyz = newCenter[x].split(" ");
					abc = oldCenter[x].split(" ");

					newCenterDouble = convertToDouble(xyz);
					oldCenterDouble = convertToDouble(abc);

					for(int z =0; z<10; z++)
						SumDiff += Math.pow((oldCenterDouble[z]-newCenterDouble[z]),2); 
					SumDiff = Math.sqrt(SumDiff);

				}

				if(SumDiff<=0.1)
					converge = true;

				Put put = new Put(Bytes.toBytes("row"));
				put.add(Bytes.toBytes("Centroids"), Bytes.toBytes("centers"), Bytes.toBytes(centerStr));
				context.getCounter(HbaseTest.Kmeans.count).increment(1L);
				context.write(null, put);
			}
		}
	} 	



	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		File f = new File("center.txt");
		if(f.exists())
			f.delete();
		FileUtils.deleteDirectory(new File("out"));

		@SuppressWarnings("unused")
		HbaseTest sent = new HbaseTest(Integer.parseInt(args[1]));



		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		Job job = new Job(conf,"hbasemapreduce");
		job.setJarByClass(HbaseTest.class);
		job.setMapperClass(map1.class);
		job.setReducerClass(reduce1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path("out"));

		job.waitForCompletion(true);

		System.out.println("1st mapper reducer done!!");


		//hbase and 2nd MapReduce
		Configuration conf1 = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf1);


		if(admin.tableExists("input")){
			admin.disableTable("input");
			admin.deleteTable("input");}

		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("input"));

		tableDescriptor.addFamily(new HColumnDescriptor("Area"));
		tableDescriptor.addFamily(new HColumnDescriptor("Property"));

		admin.createTable(tableDescriptor);
		admin.close();
		Job job1 = new Job(conf1,"HBase_Bulk_loader");        
		job1.setJarByClass(HbaseTest.class);
		job1.setMapperClass(map2.class); 
		job1.setMapOutputKeyClass(Text.class);  
		job1.setMapOutputValueClass(Text.class);  

		TableMapReduceUtil.initTableReducerJob("input", reduce2.class, job1);
		job1.setNumReduceTasks(1);
		FileInputFormat.setInputPaths(job1,new Path("out/part-r-00000"));          

		job1.waitForCompletion(true);


		System.out.println("2nd mapper reducer done!!");



		// hbase map reduce for "center"

		Configuration conf2 = HBaseConfiguration.create();
		HBaseAdmin admin2 = new HBaseAdmin(conf2);

		if(admin2.tableExists("center")){
			admin2.disableTable("center");
			admin2.deleteTable("center");
		}

		HTableDescriptor tableDescriptor2 = new HTableDescriptor(TableName.valueOf("center"));
		tableDescriptor2.addFamily(new HColumnDescriptor("Centroids"));

		admin2.createTable(tableDescriptor2);
		admin2.close();
		System.out.println("Table created");

		Job job2 = new Job(conf2,"Hbase_center");
		job2.setJarByClass(HbaseTest.class);

		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);

		TableMapReduceUtil.initTableMapperJob(
				"input",        
				scan,           
				map3.class,     
				Text.class,         
				Text.class,  
				job2);

		TableMapReduceUtil.initTableReducerJob("center", reduce3.class, job2);
		job2.setNumReduceTasks(1);   

		boolean c = job2.waitForCompletion(true);
		if (!c) {
			throw new IOException("error with job!");
		}
		System.out.println("3rd mapper reducer done!!");  



		// multipe scan mapper and reducer

		while(ItrCount<100 && !converge){

			Configuration conf3 = HBaseConfiguration.create();
			Job job3 = new Job(conf3,"Hbase_iterative_k_means");
			job3.setJarByClass(HbaseTest.class);

			List scans = new ArrayList();

			Scan scan1 = new Scan();
			scan1.setCaching(500);
			scan1.setCacheBlocks(false);
			scan1.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME,Bytes.toBytes("center"));
			scans.add(scan1);

			Scan scan2 = new Scan();
			scan2.setCaching(500);
			scan2.setCacheBlocks(false);
			scan2.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("input"));
			scans.add(scan2);

			TableMapReduceUtil.initTableMapperJob(
					scans, 
					map4.class, 
					IntWritable.class,
					Text.class, 
					job3);

			TableMapReduceUtil.initTableReducerJob("center", reduce4.class, job3);
			job3.setNumReduceTasks(1);   

			boolean d = job3.waitForCompletion(true);
			if (!d) {
				throw new IOException("error with job!");
			}
			System.out.println("4th mapper reducer done!!");
			ItrCount += job3.getCounters().findCounter(HbaseTest.Kmeans.count).getValue();

		}

	}
}

