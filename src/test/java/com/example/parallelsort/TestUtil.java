package com.example.parallelsort;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.junit.Ignore;

import com.example.parallelsort.IOUtils.IntReader;
import com.example.parallelsort.IOUtils.IntWriter;

/**
 * Utility methods used for testing.
 * 
 * @author IVotinov
 */
@Ignore
public class TestUtil {
	public static void main(String[] args) throws IOException {
		createFile(IntGenerator.DESC, 100 * 1024 * 1024);
	}
	
	/**
	 * Creates a temporary file containing {@code count} integers provided by given {@link IntGenerator}.
	 * 
	 * @param generator file will be filled with integers provided by this generator 
	 * @param count number of integers to write
	 * @return
	 * @throws IOException
	 */
	public static File createFile(IntGenerator generator, long count) throws IOException {
		File file = IOUtils.createTempFile();
		generator.reset();
		IntWriter iw = new IntWriter(file, 0);
		try {
			for (long l = 0; l < count; l++) {
				iw.write(generator.getNext());
			}
		} finally {
			iw.close();
		}
		return file;
	}

	/**
	 * Checks that given file contains integers stored in ascending order.
	 * If it's not {@link AssertionError} will be thrown.
	 * 
	 * @param file sorted file to check
	 * @param count number of integers stored in the file
	 * @param sequentialNumbers true if each next integer must be an increment of previous one, false otherwise
	 * @throws IOException
	 * @throws AssertionError if integers in the file are not in ascending order
	 */
	public static void checkAsc(File file, long count, boolean sequentialNumbers) throws IOException, AssertionError {
		int value = 0;
		IntReader ir = null;
		try {
			ir = new IntReader(file, 0, file.length() / IOUtils.INT_SIZE);
			int prevValue = ir.next();
			long c = 1;
			while (ir.hasNext()) {
				value = ir.next();
				if (sequentialNumbers) {
					assertEquals("step: " + c, prevValue + 1, value);
				} else {
					assertTrue(value >= prevValue);
				}
				prevValue = value;
				c++;
			}
			assertEquals(count, c);
		} finally {
			if (ir != null) {
				ir.close();
			}
		}
	}
	
	/**
	 * Generates a sequence of integers.
	 * {@code reset()} method must be called before first use.
	 * 
	 * @author IVotinov
	 */
	public enum IntGenerator {
		/**
		 * Generates ascending values starting from 0.
		 */
		ASC {
			private int value;
			
			@Override
			public int getNext() {
				return value++;
			}

			@Override
			public void reset() {
				value = 0;
			}
		}, 
		/**
		 * Generates descending values starting from {@link Integer#MAX_VALUE}.
		 */
		DESC {
			private int value;
			
			@Override
			public int getNext() {
				return value--;
			}

			@Override
			public void reset() {
				value = Integer.MAX_VALUE;
			}
		},
		/**
		 * Generates random integers.
		 */
		RANDOM {
			private Random random = null;
			
			@Override
			public int getNext() {
				return random.nextInt();
			}

			@Override
			public void reset() {
				random = new Random();
			}
		};
		
		/**
		 * Returns next integer in sequence.
		 * 
		 * @return next integer in sequence.
		 */
		public abstract int getNext();
		
		/**
		 * Resets this generator to the start position.
		 */
		public abstract void reset();
	}
}
