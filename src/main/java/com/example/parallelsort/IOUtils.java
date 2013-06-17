package com.example.parallelsort;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.util.NoSuchElementException;

/**
 * Utility methods to operate with file I/O.
 * 
 * @author IVotinov
 */
public final class IOUtils {
	/**
	 * Buffer size in bytes.
	 */
	public static final int BUFFER_SIZE = 256 * 1024;
	/**
	 * int size in bytes.
	 */
	public static final int INT_SIZE = 4;
	/**
	 * Byte order used to store integers.
	 */
	private static final ByteOrder BYTE_ORDER = ByteOrder.BIG_ENDIAN;

	private static final String TMP_FILE_PREFIX = "ParallelSortTest";
	
	private IOUtils() {
	}

	/**
	 * Creates a temporary file in default temporary-file directory.
	 * 
	 * @return created temporary file
	 * @throws IOException
	 */
	public static File createTempFile() throws IOException {
		return File.createTempFile(TMP_FILE_PREFIX, null);
	}
	
	/**
	 * Writes given {@code value} to {@code offset} position of {@code arr} byte array.
	 * As a result value is converted to 4 bytes in the array.
	 * 
	 * @param value int value to set
	 * @param arr byte array to set value to
	 * @param offset offset in bytes
	 */
	public static void pack(int value, byte[] arr, int offset) {
		ByteBuffer.wrap(arr, offset, INT_SIZE).order(BYTE_ORDER).putInt(value);
	}

	/**
	 * Reads integer value from {@code offset} position of given byte array.
	 * 
	 * @param arr byte array to read value from
	 * @param offset offset in bytes
	 * @return value read
	 */
	public static int unpack(byte[] arr, int offset) {
		return ByteBuffer.wrap(arr, offset, INT_SIZE).order(BYTE_ORDER).getInt();
	}

	/**
	 * Creates a buffered input stream with {@link IOUtils#BUFFER_SIZE} byte buffer.
	 * Stream is started at given {@code offset} in {@code file}.
	 * 
	 * @param file input file
	 * @param startNum index of integer to start from
	 * @return created buffered input stream
	 * @throws IOException
	 */
	public static InputStream createBufferedInputStreamWithOffset(File file, long startNum) throws IOException {
		FileInputStream fis = new FileInputStream(file);
		fis.skip(startNum * INT_SIZE);
		return new BufferedInputStream(fis, BUFFER_SIZE);
	}
		
	/**
	 * Reads {@code count} integers from {@code file} starting from {@code startNum} integer.
	 * 
	 * @param file input file
	 * @param startNum index of integer to start from
	 * @param count number of integers to read
	 * @return byte array containing read values
	 * @throws IOException
	 */
	public static byte[] readBufferFromFile(File file, long startNum, int count) throws IOException {
		byte[] buffer = new byte[count * INT_SIZE];
		InputStream is = createBufferedInputStreamWithOffset(file, startNum);
		try {
			is.read(buffer);
		} finally {
			is.close();
		}
		return buffer;
	}
	
	/**
	 * Writes provided {@code buffer} to {@code file} starting at {@code offset}.
	 * 
	 * @param file output file
	 * @param buffer buffer to write
	 * @param offset offset in bytes
	 * @throws IOException
	 */
	public static void writeBufferToFile(File file, byte[] buffer, long offset) throws IOException {
		RandomAccessFile raf = new RandomAccessFile(file, "rw");
		try {
			raf.seek(offset);
			raf.write(buffer);
		} finally {
			raf.close();
		}
	}
	
	/**
	 * Converts given byte array to int array.
	 * 
	 * @param buffer byte buffer to convert
	 * @return converted int array
	 */
	public static int[] convertToIntArray(byte[] buffer) {
		IntBuffer intBuffer = ByteBuffer.wrap(buffer).order(BYTE_ORDER).asIntBuffer(); 
		int[] array = new int[buffer.length / INT_SIZE];
		intBuffer.get(array);
		return array;
	}
	
	/**
	 * Puts all values from int array to provided byte array.
	 * 
	 * @param array int array to save to buffer
	 * @param buffer buffer to save to
	 */
	public static void putToBuffer(int[] array, byte[] buffer) {
		ByteBuffer.wrap(buffer).order(BYTE_ORDER).asIntBuffer().put(array); 
	}

	/**
	 * Copies a block of {@code count} integers starting from given index 
	 * from {@code in} file to the same position in {@code out} file.
	 * This method is used when data block has no pair to be merged with at current step.
	 * 
	 * @param in input file
	 * @param out output file
	 * @param startNum index of integer to start from
	 * @param count number of integers to copy
	 * @throws IOException
	 */
	public static void copyBlock(File in, File out, long startNum, long count) throws IOException {
		RandomAccessFile raf = new RandomAccessFile(out, "rw");
		try {
			FileInputStream fis = new FileInputStream(in);
			try {
				FileChannel fc = fis.getChannel().position(startNum * INT_SIZE);
				raf.getChannel().transferFrom(fc, startNum * INT_SIZE, count * INT_SIZE);
			} finally {
				fis.close();
			}
		} finally {
			raf.close();
		}
	}
	
	/**
	 * Utility class to read integers from the underlying file.
	 * This class uses buffer of {@link IOUtils#BUFFER_SIZE} bytes to reduce I/O operations count. 
	 * 
	 * @author IVotinov
	 */
	public static class IntReader implements Closeable {
		private InputStream is;
		private byte[] intBytes = new byte[INT_SIZE];
		private long count;
		private long index = 0;
		private boolean hasCahedValue = false;
		private int cachedValue;

		/**
		 * Constructs new IntReader for given file, start position and count.
		 * 
		 * @param file input file
		 * @param startNum index of integer to start writing from
		 * @param count number of integers to read
		 * @throws IOException
		 */
		public IntReader(File file, long startNum, long count) throws IOException {
			is = createBufferedInputStreamWithOffset(file, startNum);
			this.count = count;
		}
		
		/**
		 * Returns true if there are elements left to read.
		 * 
		 * @return true if there are elements left to read
		 */
		public boolean hasNext() {
			return hasCahedValue || index < count;
		}
		
		/**
		 * Retrieves next value from this reader.
		 * If there are no more elements to read {@link NoSuchElementException} is thrown.
		 * 
		 * @return next value
		 * @throws IOException
		 */
		public int next() throws IOException {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			if (hasCahedValue) {
				hasCahedValue = false;
				return cachedValue;
			} else {
				is.read(intBytes);
				index++;
				return unpack(intBytes, 0);
			}
		}

		/**
		 * Retrieves, but does not remove, next value from this reader.
		 * 
		 * @return next value
		 * @throws IOException
		 */
		public int peek() throws IOException {
			if (!hasCahedValue) {
				cachedValue = next();
				hasCahedValue = true;
			}
			return cachedValue;
		}
		
		/**
		 * Closes underlying file.
		 */
		public void close() throws IOException {
			is.close();
		}
	}
	
	/**
	 * Utility class to write integers to the underlying file.
	 * This class uses buffer of {@link IOUtils#BUFFER_SIZE} bytes to reduce I/O operations count. 
	 * 
	 * @author IVotinov
	 */
	public static class IntWriter implements Closeable {
		private RandomAccessFile raf;
		private byte[] buffer = new byte[BUFFER_SIZE];
		private int index = 0;
		
		/**
		 * Constructs new {@code IntWriter} for given file and start position.
		 * 
		 * @param file output file
		 * @param startNum index of integer to start writing from
		 * @throws IOException
		 */
		public IntWriter(File file, long startNum) throws IOException {
			raf = new RandomAccessFile(file, "rw");
			raf.seek(startNum * INT_SIZE);
		}
		
		/**
		 * Writes next value to the buffer. 
		 * If buffer gets full when it's written to the file and the buffer is cleared.
		 * 
		 * @param value value to write
		 * @throws IOException
		 */
		public void write(int value) throws IOException {
			pack(value, buffer, index);
			index += INT_SIZE;
			if (index == buffer.length) {
				raf.write(buffer);
				index = 0;
			}
		}
		
		/**
		 * Writes remaining of the buffer to the file and closes it.
		 */
		public void close() throws IOException {
			raf.write(buffer, 0, index);
			raf.close();
		}
	}
}
