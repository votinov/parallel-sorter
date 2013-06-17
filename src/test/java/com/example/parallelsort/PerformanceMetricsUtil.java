package com.example.parallelsort;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.junit.Ignore;

import com.example.parallelsort.ParallelSorter;
import com.example.parallelsort.TestUtil.IntGenerator;

/**
 * Utility class for gathering statistics on parallel sort running time
 * depending on input file size and thread count.
 * 
 * @author IVotinov
 */
@Ignore
public final class PerformanceMetricsUtil {
	/**
	 * File sizes to test in bytes.
	 */
	private static final long[] SIZES_TO_TEST =
		{10 * 1024,
		 1024 * 1024, 
		 10 * 1024 * 1024,
		 100 * 1024 * 1024,
		 400 * 1024 * 1024, 
		 1024 * 1024 * 1024};
	
	/**
	 * Thread counts to test.
	 */
	private static final int[] THREADS_COUNT_TO_TEST =
		{1,
		2,
		4,
		8,
		16,
		50,
		100};

	/**
	 * number of times each configuration will be tested to get average time.
	 */
	private static final int RUNS_COUNT = 5;
	
	private static final String DEFAULT_OUTPUT_FILE = "results.csv";
	private static final String SEPARATOR = ";";
	
	private PerformanceMetricsUtil() {
	}
	
	/**
	 * Main method to gather statistics and write them to output file in CSV format.
	 * 
	 * @param args may contain output filename parameter
	 * @throws IOException
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
		String filename = DEFAULT_OUTPUT_FILE;
		if (args.length != 0) {
			filename = args[0];
		}
		
		BufferedWriter bw = null; 
		try {
			bw = new BufferedWriter(new FileWriter(filename));
			writeHeader(bw);
			writeMetrics(bw);
		} finally {
			if (bw != null) {
				bw.close();
			}
		}
	}
	
	/**
	 * Writes table header to given writer.
	 * 
	 * @param w writer to which header will be written
	 * @throws IOException
	 */
	private static void writeHeader(BufferedWriter w) throws IOException {
		w.write("Size\\Threads");
		for (int i = 0; i < THREADS_COUNT_TO_TEST.length; i++) {
			w.write(SEPARATOR);
			w.write(String.valueOf(THREADS_COUNT_TO_TEST[i]));
		}
		w.newLine();
	}	
	
	/**
	 * Writes metrics table to given writer.
	 * 
	 * @param w writer to which metrics will be written
	 * @throws IOException
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	private static void writeMetrics(BufferedWriter w) throws IOException, InterruptedException, ExecutionException {
		for (int i = 0; i < SIZES_TO_TEST.length; i++) {
			long size = SIZES_TO_TEST[i];
			w.write(String.valueOf(size));
			for (int j = 0; j < THREADS_COUNT_TO_TEST.length; j++) {
				int threadCount = THREADS_COUNT_TO_TEST[j];
				w.write(SEPARATOR);
				long avgTimeMs = getAvgSortTime(size, threadCount);
				w.write(String.valueOf(avgTimeMs));
				//this is added to see results in the file immediately
				w.flush();
			}
			w.newLine();
		}
	}
	
	/**
	 * Computes average sort time for given input file size and thread count.
	 * 
	 * @param size input file size in bytes
	 * @param threadCount number of threads used in sorting 
	 * @return average running time in milliseconds
	 * @throws IOException
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	private static long getAvgSortTime(long size, int threadCount) throws IOException, InterruptedException, ExecutionException {
		long sum = 0;
		for (int i = 0; i < RUNS_COUNT; i++) {
			File file = null;
			try {
				file = TestUtil.createFile(IntGenerator.DESC, size / IOUtils.INT_SIZE);
				long start = System.currentTimeMillis();
				ParallelSorter.sort(file, threadCount);
				sum += System.currentTimeMillis() - start;
			} finally {
				if (file != null) {
					file.delete();
				}
			}
		}
		return sum / RUNS_COUNT;
	}
}
