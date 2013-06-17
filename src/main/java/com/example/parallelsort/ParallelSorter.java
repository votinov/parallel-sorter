package com.example.parallelsort;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.parallelsort.IOUtils.IntReader;
import com.example.parallelsort.IOUtils.IntWriter;

/**
 * This class implements parallel sort algorithm.
 * First, input file is split into blocks that fit to memory (see {@link IOUtils#BUFFER_SIZE})
 * and each block is sorted using {@link Arrays#sort(int[])} method.
 * Then parallel merge sort is used to merge overall result.
 * A temporary file with same size as input file is created to store intermediate results. 
 * 
 * @author IVotinov
 */
public final class ParallelSorter {
	private static final Logger LOG = LoggerFactory.getLogger(ParallelSorter.class);
	
	private ParallelSorter() {
	}
	 
	/**
	 * Sorts given file in parallel using given number of threads.
	 * Input file size MUST be a multiple of 4.
	 * NOTE: call to this method MAY recreate {@code in} file. 
	 * 
	 * @param in input file
	 * @param threadCount number of threads
	 * @throws IOException
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public static void sort(File in, int threadCount) throws IOException, ExecutionException, InterruptedException {
		ExecutorService executor = Executors.newFixedThreadPool(threadCount);
		try{
			// creating a temporary file to store intermediate results
			// file size will be the same as provided input file
			File out = IOUtils.createTempFile();
	
			long count = in.length() / IOUtils.INT_SIZE;
			int blockSize = IOUtils.BUFFER_SIZE / IOUtils.INT_SIZE;
			int blocksCount = (int) (count / blockSize + (count % blockSize != 0 ? 1 : 0));
			
			// submit in-memory sort tasks to the executor
			// returned list of Futures is used to keep track of tasks dependencies
			List<Future<Integer>> sortTasks = initialSort(executor, in, out, count, blockSize, blocksCount);
			
			// submit merge tasks to the executor
			Future<Integer> lastTask = mergeAll(executor, sortTasks, in, out, count, blockSize, blocksCount);
			
			// waiting for all tasks completion
			// passesCount will contain number of merge passes performed
			int passesCount = 0;
			try {
				passesCount = lastTask.get();
			} catch (ExecutionException e) {
				handleException(e);
			}
			
			cleanup(in, out, passesCount);
		} finally {
			executor.shutdown();
		}
	}

	/**
	 * Performs cleanup.
	 * In case when last merge ended up with data in initial file, temporary file is deleted.
	 * In case when last merge ended up with data in temporary file, 
	 * initial file is deleted and temporary file is renamed to initial file name.
	 * 
	 * @param in input file
	 * @param tmp temporary file created for sorting
	 * @param passesCount number of merge passes performed
	 */
	private static void cleanup(File in, File tmp, int passesCount) {
		if (passesCount % 2 == 0) {
			in.delete();
			tmp.renameTo(in);
		} else {
			tmp.delete();
		}
	}

	/**
	 * Splits input file into blocks and creates {@link InMemorySortTask} for each block.
	 * Tasks are submitted to provided {@link ExecutorService} and will be executed asynchronously.
	 * 
	 * @param es {@link ExecutorService} that will execute tasks
	 * @param in input file
	 * @param out created temporary file
	 * @param count total count of integers in the input file
	 * @param blockSize number of integers fitting in block
	 * @param blocksCount number of blocks
	 * @return list of {@link Future}'s for created in-memory sort tasks
	 */
	private static List<Future<Integer>> initialSort(ExecutorService es, File in, File out, 
			long count, int blockSize, int blocksCount) {
		List<Future<Integer>> result = new ArrayList<Future<Integer>>(blocksCount);
		for (int i = 0; i < blocksCount; i++) {
			long c = blockSize;
			// if it is the last block it may be shorter
			if (i == blocksCount - 1 && count % blockSize != 0) {
				c = count % blockSize;
			}
			result.add(es.submit(new InMemorySortTask(in, out, i * blockSize, (int) c)));
		}
		return result;
	}

	/**
	 * Creates {@link MergeTask} tasks and submits them to the provided {@link ExecutorService}. 
	 * Merge is performed in passes.
	 * On each pass remaining sorted blocks are split into pairs, 
	 * and each pair is merged into one sorted block. 
	 * So each merge pass generally consists of multiple {@link MergeTask}s. 
	 * If the number of blocks on current pass is odd, the last block is simply copied to other file 
	 * to ensure that all data is in the same file before the next pass.
	 * On each pass already sorted blocks are in one file and result is written to the other file.
	 * On next pass in and out files are swapped.
	 * Tasks created by this method are executed asynchronously. 
	 * {@link Future}s for previous pass tasks are used to keep track of task dependencies 
	 * (see {@link MergeTask} and {@link CopyBlockTask} for details).
	 * Method returns last merge task, after this task completion the file will be sorted.
	 * 
	 * @param es {@link ExecutorService} that will execute tasks
	 * @param sortTasks {@link Future}s for created in-memory sort tasks
	 * @param in input file
	 * @param out created temporary file
	 * @param count total count of integers in the input file
	 * @param initialBlockSize number of integers fitting in initial (in-memory sort) block
	 * @param initialBlocksCount initial number of blocks
	 * @return last merge task
	 */
	private static Future<Integer> mergeAll(ExecutorService es, List<Future<Integer>> sortTasks, File in, File out, 
			long count, int initialBlockSize, int initialBlocksCount) {
		File inFile = out;
		File outFile = in;
		int blocksCount = initialBlocksCount;
		int blockSize = initialBlockSize;
		
		List<Future<Integer>> prevPassTasks = sortTasks;
		
		while (blocksCount > 1) {
			// starting a new merge pass
			List<Future<Integer>> currentPassTasks = new ArrayList<Future<Integer>>();
			// processing remaining blocks in pairs
			for (int i = 0; i < blocksCount; i += 2) {
				long c = blockSize;
				// this is last block and it's odd so it doesn't have a pair
				if (i == blocksCount - 1) {
					// also it can be shorter than regular
					if (count % blockSize != 0) {
						c = count % blockSize;
					}
					// adding copy task
					currentPassTasks.add(es.submit(new CopyBlockTask(inFile, outFile, i * blockSize, c, prevPassTasks.subList(i, i + 1))));
				} else {
					// if it's last block it can be shorter
					if (i == blocksCount - 2 && count % blockSize != 0) {
						c = count % blockSize;
					}
					// adding merge task
					currentPassTasks.add(es.submit(new MergeTask(inFile, outFile, i * blockSize, blockSize, c, prevPassTasks.subList(i, i + 2))));
				}
			}
			
			blockSize = 2 * blockSize;
			blocksCount = blocksCount / 2 + (blocksCount % 2 != 0 ? 1 : 0);
			
			// swapping in and out files for the next pass 
			File t = inFile;
			inFile = outFile;
			outFile = t;
			prevPassTasks = currentPassTasks;
		}
		return prevPassTasks.get(0);
	}
	
	/**
	 * Asynchronous task for in-memory sort of part of input file.
	 * This task returns 0 as a result, so in-memory sort can be considered as 0th merge pass
	 * (see {@link MergeTask} and {@link CopyBlockTask} for more info on task results).
	 * 
	 * @author IVotinov
	 */
	private static class InMemorySortTask implements Callable<Integer> {
		private File in;
		private File out;
		private long startNum;
		private int count;
		
		/**
		 * Constructs new InMemorySortTask.
		 * 
		 * @param in input file
		 * @param out output file
		 * @param startNum index of integer to start from
		 * @param count count of integers to sort
		 */
		public InMemorySortTask(File in, File out, long startNum, int count) {
			this.in = in;
			this.out = out;
			this.startNum = startNum;
			this.count = count;
		}

		/**
		 * {@inheritDoc}
		 */
		public Integer call() throws IOException {
			sortInMemory(in, out, startNum, count);
			return 0;
		}
	}
	
	/**
	 * Asynchronous task to merge two consequent blocks from input file to the same position in the output file.
	 * Each merge task depends on data provided by two previous pass merge tasks,
	 * or two in-memory sort tasks if this is the first merge pass.
	 * {@link Future}s provided in constructor are used to wait for these tasks completion.
	 * Task returns current merge pass number.
	 * This result is an increment of dependency task result. 
	 * 
	 * @author IVotinov
	 */
	private static class MergeTask implements Callable<Integer> {
		private File in;
		private File out;
		private long startNum;
		private long count1;
		private long count2;
		private List<Future<Integer>> futuresToWait;

		/**
		 * Constructs new MergeTask.
		 * 
		 * @param in input file
		 * @param out output file
		 * @param startNum index of integer to start from
		 * @param count1 number of integers to read from first block
		 * @param count2 number of integers to read from second block
		 * @param futuresToWait futures for tasks on whose completion this task depends
		 */
		public MergeTask(File in, File out, long startNum, long count1, long count2, List<Future<Integer>> futuresToWait) {
			this.in = in;
			this.out = out;
			this.startNum = startNum;
			this.count1 = count1;
			this.count2 = count2;
			this.futuresToWait = futuresToWait;
		}

		/**
		 * {@inheritDoc}
		 */
		public Integer call() throws IOException, ExecutionException, InterruptedException {
			int result = waitForFutures(futuresToWait);
			merge(in, out, startNum, count1, count2);
			return result + 1;
		}
	}
	
	/**
	 * Asynchronous task to copy a block of {@code count} integers starting from given index 
	 * from {@code in} file to the same position in {@code out} file.
	 * Each copy block task depends on data provided by one previous pass merge task,
	 * one previous pass copy block task, or one in-memory sort task if this is the first merge pass.
	 * {@link Future} provided in constructor is used to wait for such tasks completion.
	 * Task returns current merge pass number.
	 * This result is an increment of dependency task result.
	 *  
	 * @author IVotinov
	 */
	private static class CopyBlockTask implements Callable<Integer> {
		private File in;
		private File out;
		private long startNum;
		private long count;
		private List<Future<Integer>> futuresToWait;

		/**
		 * Constructs new CopyBlockTask.
		 * 
		 * @param in input file
		 * @param out output file
		 * @param startNum index of integer to start from
		 * @param count number of integers to copy
		 * @param futuresToWait futures for tasks on whose completion this task depends
		 */
		public CopyBlockTask(File in, File out, long startNum, long count, List<Future<Integer>> futuresToWait) {
			this.in = in;
			this.out = out;
			this.startNum = startNum;
			this.count = count;
			this.futuresToWait = futuresToWait;
		}

		/**
		 * {@inheritDoc}
		 */
		public Integer call() throws IOException, ExecutionException, InterruptedException {
			int result = waitForFutures(futuresToWait);
			IOUtils.copyBlock(in, out, startNum, count);
			return result + 1;
		}
	}
	
	/**
	 * Waits for completion of a list of tasks.
	 * This method returns the result of last task in the list.
	 * 
	 * In case when one of the tasks thrown an exception, this method re-throws exception's cause. 
	 * If multiple tasks return exceptions this method will throw exception from the first task in list that fired exception. 
	 * 
	 * @param futures tasks to wait for
	 * @return result of the last task in the list
	 * @throws IOException
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	private static int waitForFutures(List<Future<Integer>> futures) throws IOException, ExecutionException, InterruptedException {
		int result = 0;
		for (Future<Integer> f : futures) {
			try {
				result = f.get();
			} catch (ExecutionException e) {
				handleException(e);
			}
		}
		return result;
	}
	
	/**
	 * Logs exception and re-throws it's cause.
	 * This is done to prevent multiple ExecutionException wrappers stacking through dependent tasks tree.
	 * 
	 * @param e ExecutionException to handle
	 * @throws IOException
	 * @throws ExecutionException
	 */
	private static void handleException(ExecutionException e) throws IOException, ExecutionException {
		LOG.error("Unexpected exception", e);
		if (e.getCause() != null) {
			if (e.getCause() instanceof IOException) {
				throw (IOException) e.getCause();
			} else if (e.getCause() instanceof RuntimeException) {
				throw (RuntimeException) e.getCause();
			}
		}
		throw e;
	}
	
	/**
	 * Merges two consequent blocks from input file to the same position in the output file.
	 * 
	 * @param in input file
	 * @param out output file
	 * @param startNum index of integer to start from
	 * @param count1 number of integers to read from first block
	 * @param count2 number of integers to read from second block
	 * @throws IOException
	 */
	private static void merge(File in, File out, long startNum, long count1, long count2) throws IOException {
		IntReader ir1 = new IntReader(in, startNum, count1);
		try {
			IntReader ir2 = new IntReader(in, startNum + count1, count2);
			try {
				IntWriter iw = new IntWriter(out, startNum);
				try {
					doMerge(ir1, ir2, iw);
				} finally {
					iw.close();
				}
			} finally {
				ir2.close();
			}
		} finally {
			ir1.close();
		}
	}

	/**
	 * Merges two blocks into one using provided {@link IOUtils.IntReader}s and {@link IOUtils.IntWriter}.
	 * 
	 * @param ir1 IntReader for first input block
	 * @param ir2 IntReader for second input block
	 * @param iw IntWriter for resulting block
	 * @throws IOException
	 */
	private static void doMerge(IntReader ir1, IntReader ir2, IntWriter iw) throws IOException {
		while (ir1.hasNext() && ir2.hasNext()) {
			if (ir1.peek() <= ir2.peek()) {
				iw.write(ir1.next());
			} else {
				iw.write(ir2.next());
			}
		}

		while (ir1.hasNext()) {
			iw.write(ir1.next());
		}

		while (ir2.hasNext()) {
			iw.write(ir2.next());
		}
	}
	
	/**
	 * Performs in-memory sort of block of {@code in} file and writes result to {@code out} file.
	 * In-memory sorting is performed using {@link Arrays#sort(int[])} method.
	 * @param in input file
	 * @param out output file
	 * @param startNum index of integer to start from
	 * @param count count of integers to sort
	 * @throws IOException
	 */
	private static void sortInMemory(File in, File out, long startNum, int count) throws IOException {
		byte[] buffer = IOUtils.readBufferFromFile(in, startNum, count);
		int[] array = IOUtils.convertToIntArray(buffer);
		
		Arrays.sort(array);

		IOUtils.putToBuffer(array, buffer);
		IOUtils.writeBufferToFile(out, buffer, startNum * IOUtils.INT_SIZE);
	}

	/**
	 * Main method, sorts given input stream using specified number of threads.
	 * 
	 * @param args input filename and number of threads to use for sorting
	 */
	public static void main(String[] args) {
		if (args.length < 2) {
			throw new IllegalArgumentException("You have to specify input file and number of threads");
		}
		String filename = args[0];
		File in = new File(filename);
		int threadCount = Integer.parseInt(args[1]);
		try {
			LOG.info("Sorting {} file using {} threads", in.getAbsolutePath(), String.valueOf(threadCount));
			long start = System.currentTimeMillis();
			sort(in, threadCount);
			LOG.info("Sorted {} file in {} ms", in.getAbsolutePath(), String.valueOf(System.currentTimeMillis() - start));
		} catch (IOException e) {
			LOG.error("Unexpected exception: ", e);
		} catch (ExecutionException e) {
			LOG.error("Unexpected exception: ", e);
		} catch (InterruptedException e) {
			LOG.error("Unexpected exception: ", e);
		}
	}
}
