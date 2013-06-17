package com.example.parallelsort;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import junit.framework.TestCase;

import com.example.parallelsort.TestUtil.IntGenerator;

import org.junit.Test;

public class ParrallelSorterTest extends TestCase {
	// number of threads to test on
	private static final int THREADS_COUNT = 4;
	
	// number of integers:
	// sort ends with result in input file
	private static final long SAME_FILE_COUNT = 8 * IOUtils.BUFFER_SIZE;
	// sort ends with result in input file, plus one extra block on the first merge step
	private static final long SAME_FILE_WITH_EXCESS_BLOCK_START_COUNT = 4 * IOUtils.BUFFER_SIZE + 1;
	// sort ends with result in input file, plus one extra block on the middle merge step
	private static final long SAME_FILE_WITH_EXCESS_BLOCK_MIDDLE_COUNT = 6 * IOUtils.BUFFER_SIZE;

	// sort ends with result in created temp file
	private static final long OTHER_FILE_COUNT = 4 * IOUtils.BUFFER_SIZE;
	// sort ends with result in created temp file, plus one extra block on the first merge step
	private static final long OTHER_FILE_WITH_EXCESS_BLOCK_START_COUNT = 8 * IOUtils.BUFFER_SIZE + 1;
	// sort ends with result in created temp file, plus one extra block on the middle merge step
	private static final long OTHER_FILE_WITH_EXCESS_BLOCK_MIDDLE_COUNT = 12 * IOUtils.BUFFER_SIZE;
	
	// file smaller than in-memory sort buffer
	private static final long SMALL_COUNT = 10;

	/**
	 * Tests sort method on file with descending values. 
	 * 
	 * @throws IOException
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	@Test
	public void testDesc() throws IOException, InterruptedException, ExecutionException {
		doTest(IntGenerator.DESC, SAME_FILE_COUNT, true);
		doTest(IntGenerator.DESC, SAME_FILE_WITH_EXCESS_BLOCK_START_COUNT, true);
		doTest(IntGenerator.DESC, SAME_FILE_WITH_EXCESS_BLOCK_MIDDLE_COUNT, true);
		doTest(IntGenerator.DESC, OTHER_FILE_COUNT, true);
		doTest(IntGenerator.DESC, OTHER_FILE_WITH_EXCESS_BLOCK_START_COUNT, true);
	}

	/**
	 * Tests sort method on already sorted file.
	 * 
	 * @throws IOException
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	@Test
	public void testAsc() throws IOException, InterruptedException, ExecutionException {
		doTest(IntGenerator.ASC, SAME_FILE_COUNT, true);
		doTest(IntGenerator.ASC, SAME_FILE_WITH_EXCESS_BLOCK_START_COUNT, true);
		doTest(IntGenerator.ASC, SAME_FILE_WITH_EXCESS_BLOCK_MIDDLE_COUNT, true);
		doTest(IntGenerator.ASC, OTHER_FILE_COUNT, true);
		doTest(IntGenerator.ASC, OTHER_FILE_WITH_EXCESS_BLOCK_START_COUNT, true);
		doTest(IntGenerator.ASC, OTHER_FILE_WITH_EXCESS_BLOCK_MIDDLE_COUNT, true);
	}

	/**
	 * Tests sort on file with random values.
	 * 
	 * @throws IOException
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	@Test
	public void testRandom() throws IOException, InterruptedException, ExecutionException {
		doTest(IntGenerator.RANDOM, SAME_FILE_COUNT, false);
		doTest(IntGenerator.RANDOM, SAME_FILE_WITH_EXCESS_BLOCK_START_COUNT, false);
		doTest(IntGenerator.RANDOM, SAME_FILE_WITH_EXCESS_BLOCK_MIDDLE_COUNT, false);
		doTest(IntGenerator.RANDOM, OTHER_FILE_COUNT, false);
		doTest(IntGenerator.RANDOM, OTHER_FILE_WITH_EXCESS_BLOCK_START_COUNT, false);
		doTest(IntGenerator.RANDOM, OTHER_FILE_WITH_EXCESS_BLOCK_MIDDLE_COUNT, false);
	}

	/**
	 * Tests sorting on a file smaller than buffer used for in memory testing.
	 * 
	 * @throws IOException
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	@Test
	public void testSmallSize() throws IOException, InterruptedException, ExecutionException {
		doTest(IntGenerator.DESC, SMALL_COUNT, true);
	}
	
	/**
	 * Creates a temp file containing {@code count} integers provided by given {@link IntGenerator}, 
	 * sorts it and checks that resulting file contains values sorted in ascending order.
	 * 
	 * @param generator test file will be filled with integers provided by this generator
	 * @param count number of integers to store in the input file
	 * @param sequentialNumbers true if each next integer must be an increment of previous one, false otherwise
	 * @throws IOException
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	private void doTest(IntGenerator generator, long count, boolean sequentialNumbers) throws IOException, InterruptedException, ExecutionException {
		File file = null;
		try {
			file = TestUtil.createFile(IntGenerator.DESC, count);
			ParallelSorter.sort(file, THREADS_COUNT);
			TestUtil.checkAsc(file, count, sequentialNumbers);
		} finally {
			if (file != null) {
				file.delete();
			}
		}
	}
	
}
