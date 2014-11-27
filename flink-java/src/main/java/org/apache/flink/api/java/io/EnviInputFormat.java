/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.java.io;

import java.io.DataInputStream;
import java.io.File;
import java.io.FilterInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.powermock.reflect.exceptions.FieldNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.spatial.Coordinate;
import org.apache.flink.api.java.spatial.TileInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.InputViewDataInputStreamWrapper;
import org.apache.flink.util.StringUtils;

/**
 * Base class for all input formats that use blocks of fixed size. The input splits are aligned to these blocks. Without
 * configuration, these block sizes equal the native block sizes of the HDFS.
 */
public abstract class EnviInputFormat<T> extends FileInputFormat<T> {
	private static final long serialVersionUID = -6483882465613479436L;
	private static final Logger LOG = LoggerFactory.getLogger(EnviInputFormat.class);

	/**
	 * Configuration parameters: Size of each tile in pixels in both dimensions.
	 */
	public static final String PARAM_XSIZE = "input.xsize";
	public static final String PARAM_YSIZE = "input.ysize";

	private int xsize = -1, ysize = -1;
	
	private DataInputStream dataInputStream;

	
	
	public EnviInputFormat(Path path) {
		super(path);
	} 
	
	@Override
	public void configure(Configuration parameters) {
		super.configure(parameters);

		this.xsize = parameters.getInteger(PARAM_XSIZE, -1);
		if(this.xsize <= 0) {
			throw new IllegalArgumentException("Please set the xsize parameter to a positive value.");
		}
		this.ysize = parameters.getInteger(PARAM_YSIZE, -1);
		if(this.ysize <= 0) {
			throw new IllegalArgumentException("Please set the ysize parameter to a positive value.");
		}
	}

	@Override
	public FileInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		List<FileStatus> files = this.getFiles();
		final FileSystem fs = this.filePath.getFileSystem();

		final List<FileInputSplit> inputSplits = new ArrayList<FileInputSplit>(minNumSplits);
		for (FileStatus file : files) {
			// Read header file:
			FSDataInputStream fdis = fs.open(file.getPath());
			TileInfo info = new TileInfo(fdis);
			fdis.close();

			// Determine data file associated with this header:
			String interleaveType = info.getInterleaveType();
			if(!interleaveType.equals("bsq")) {
				throw new RuntimeException("Interleave type " + interleaveType + " unsupported, use bsq.");
			}
			Path dataFile = new Path(file.getPath().toUri().getPath().replaceAll("\\.hdr$", "." + interleaveType));
			FileStatus dataFileStatus;
			try {
				dataFileStatus = fs.getFileStatus(dataFile);
			} catch(FieldNotFoundException e) {
				throw new RuntimeException("Data file " + dataFile + " for header " + file + " not found.", e);
			}
			
			/*
			 *  Calculate pixel tile size: The rightmost column and lowest row of tiles may contain empty
			 *  pixels.
			 */
			int xsplits = (info.getPixelColumns() + xsize - 1) / xsize;
			int ysplits = (info.getPixelRows() + ysize - 1) / ysize;
			LOG.info("Splitting " + info.getPixelColumns() + "x" + info.getPixelRows() + " image into " +
					xsplits + "x" + ysplits + " tiles of size " + xsize + "x" + ysize + ".");
			
			// Real coordinates of this image + coordinate differences:
			Coordinate upperLeftCorner = info.getUpperLeftCoordinate();
			Coordinate realLowerRightCorner = info.getLowerRightCoordinate();
			Coordinate realDiff = upperLeftCorner.diff(realLowerRightCorner);
			// Distance between upper left corner and virtual lower right corner of the last tile, including empty pixels:
			Coordinate diff = realDiff.scale(1.0 * xsize / info.getPixelColumns(),
					1.0 * ysize / info.getPixelRows());

			for(int x = 0; x < xsplits; x++) {
				// Calculate pixel coordinate:
				int pxstart = x *  xsize; // inclusive
				int pxnext = (x + 1) *  xsize; // EXCLUSIVE

				// Calculate coordinate of leftmost and rightmost pixels

				for(int y = 0; y < ysplits; y++) {
					// Calculate pixel coordinate:
					int pystart = x *  ysize; // inclusive
					int pynext = (x + 1) *  ysize; // EXCLUSIVE

					Coordinate tileUpperLeft = upperLeftCorner.addScaled(diff, xsplits, ysplits);
					Coordinate tileLowerRight = upperLeftCorner.addScaled(diff, xsplits + 1, ysplits + 1);
					
					// Determine list of FS blocks that contain the given block
					// TODO
				}
			}
			
			// 3. add block positions for split
			// 4. remember information in EnviInputSplit?
=xsplits + "x" + ysplits + " tiles of size " + xsize + "x" + ysize + ".");
			
			// Real coordinates of this image + coordinate differences:
			Coordinate upperLeftCorner = info.getUpperLeftCoordinate();
			Coordinate realLowerRightCorner = info.getLowerRightCoordinate();
			Coordinate realDiff = upperLeftCorner.diff(realLowerRightCorner);
			// Distance between upper left corner and virtual lower right corner of the last tile, including empty pixels:
			Coordinate diff = realDiff.scale(1.0 * xsize / info.getPixelColumns(),
					1.0 * ysize / info.getPixelRows());

			for(int x = 0; x < xsplits; x++) {
				// Calculate pixel coordinate:
				int pxstart = x *  xsize; // inclusive
				int pxnext = (x + 1) *  xsize; // EXCLUSIVE

				// Calculate coordinate of leftmost and rightmost pixels

				for(int y = 0; y < ysplits; y++) {
					// Calculate pixel coordinate:
					int pystart = x *  ysize; // inclusive
					int pynext = (x + 1) *  ysize; // EXCLUSIVE

					Coordinate tileUpperLeft = upperLeftCorner.addScaled(diff, xsplits, ysplits);
					Coordinate tileLowerRight = upperLeftCorner.addScaled(diff, xsplits + 1, ysplits + 1);
					
					// Determine list of FS blocks that contain the given block
					// TODO
				}
			}
			
			// 3. add block positions for split
			// 4. remember information in EnviInputSplit?
=xsplits + "x" + ysplits + " tiles of size " + xsize + "x" + ysize + ".");
			
			// Real coordinates of this image + coordinate differences:
			Coordinate upperLeftCorner = info.getUpperLeftCoordinate();
			Coordinate realLowerRightCorner = info.getLowerRightCoordinate();
			Coordinate realDiff = upperLeftCorner.diff(realLowerRightCorner);
			// Distance between upper left corner and virtual lower right corner of the last tile, including empty pixels:
			Coordinate diff = realDiff.scale(1.0 * xsize / info.getPixelColumns(),
					1.0 * ysize / info.getPixelRows());

			for(int x = 0; x < xsplits; x++) {
				// Calculate pixel coordinate:
				int pxstart = x *  xsize; // inclusive
				int pxnext = (x + 1) *  xsize; // EXCLUSIVE

				// Calculate coordinate of leftmost and rightmost pixels

				for(int y = 0; y < ysplits; y++) {
					// Calculate pixel coordinate:
					int pystart = x *  ysize; // inclusive
					int pynext = (x + 1) *  ysize; // EXCLUSIVE

					Coordinate tileUpperLeft = upperLeftCorner.addScaled(diff, xsplits, ysplits);
					Coordinate tileLowerRight = upperLeftCorner.addScaled(diff, xsplits + 1, ysplits + 1);
					
					// Determine list of FS blocks that contain the given block
					// TODO
					// 4. remember information in EnviInputSplit?
				}
			}
			
			// 3. add block positions for split
			for (long pos = 0, length = file.getLen(); pos < length; pos += blockSize) {
				long remainingLength = Math.min(pos + blockSize, length) - pos;

				// get the block locations and make sure they are in order with respect to their offset
				final BlockLocation[] blocks = fs.getFileBlockLocations(file, pos, remainingLength);
				Arrays.sort(blocks);

				inputSplits.add(new FileInputSplit(inputSplits.size(), file.getPath(), pos, remainingLength,
					blocks[0].getHosts()));
			}
		}

		if (inputSplits.size() < minNumSplits) {
			LOG.warn(String.format(
				"With the given block size %d, the file %s cannot be split into %d blocks. Filling up with empty splits...",
				blockSize, this.filePath, minNumSplits));
			FileStatus last = files.get(files.size() - 1);
			final BlockLocation[] blocks = fs.getFileBlockLocations(last, 0, last.getLen());
			for (int index = files.size(); index < minNumSplits; index++) {
				inputSplits.add(new FileInputSplit(index, last.getPath(), last.getLen(), 0, blocks[0].getHosts()));
			}
		}

		return inputSplits.toArray(new FileInputSplit[0]);
	}

	protected List<FileStatus> getFiles() throws IOException {
		// get all the files that are involved in the splits
		List<FileStatus> files = new ArrayList<FileStatus>();

		final FileSystem fs = this.filePath.getFileSystem();
		final FileStatus pathFile = fs.getFileStatus(this.filePath);

		if (pathFile.isDir()) {
			// input is directory. list all contained files
			final FileStatus[] partials = fs.listStatus(this.filePath);
			for (FileStatus partial : partials) {
				if (!partial.isDir()) {
					files.add(partial);
				}
			}
		} else {
			files.add(pathFile);
		}
		
		List<FileStatus> headerFiles = new ArrayList<FileStatus>();
		for(FileStatus fstat: files) {
			// Only accept header files
			if(fstat.getPath().toUri().toString().endsWith(".hdr")) {
				headerFiles.add(fstat);
			}
		}
		
		return files;
	}

	@Override
	public SequentialStatistics getStatistics(BaseStatistics cachedStats) {
// TODO
		final FileBaseStatistics cachedFileStats = (cachedStats != null && cachedStats instanceof FileBaseStatistics) ?
			(FileBaseStatistics) cachedStats : null;

		try {
			final Path filePath = this.filePath;

			// get the filesystem
			final FileSystem fs = FileSystem.get(filePath.toUri());
			final ArrayList<FileStatus> allFiles = new ArrayList<FileStatus>(1);

			// let the file input format deal with the up-to-date check and the basic size
			final FileBaseStatistics stats = getFileStats(cachedFileStats, filePath, fs, allFiles);
			if (stats == null) {
				return null;
			}

			// check whether the file stats are still sequential stats (in that case they are still valid)
			if (stats instanceof SequentialStatistics) {
				return (SequentialStatistics) stats;
			}
			return createStatistics(allFiles, stats);
		} catch (IOException ioex) {
			if (LOG.isWarnEnabled()) {
				LOG.warn(String.format("Could not determine complete statistics for file '%s' due to an I/O error: %s",
					this.filePath, StringUtils.stringifyException(ioex)));
			}
		} catch (Throwable t) {
			if (LOG.isErrorEnabled()) {
				LOG.error(String.format("Unexpected problem while getting the file statistics for file '%s' due to %s",
					this.filePath, StringUtils.stringifyException(t)));
			}
		}
		// no stats available
		return null;
	}

	protected FileInputSplit[] getInputSplits() throws IOException {
		return this.createInputSplits(0);
	}

	/**
	 * Fill in the statistics. The last modification time and the total input size are prefilled.
	 * 
	 * @param files
	 *        The files that are associated with this block input format.
	 * @param stats
	 *        The pre-filled statistics.
	 */
	protected SequentialStatistics createStatistics(List<FileStatus> files, FileBaseStatistics stats)
			throws IOException {
		if (files.isEmpty()) {
			return null;
		}

		BlockInfo blockInfo = this.createBlockInfo();

		long totalCount = 0;
		for (FileStatus file : files) {
			// invalid file
			if (file.getLen() < blockInfo.getInfoSize()) {
				continue;
			}

			FSDataInputStream fdis = file.getPath().getFileSystem().open(file.getPath(), blockInfo.getInfoSize());
			fdis.seek(file.getLen() - blockInfo.getInfoSize());

			DataInputStream input = new DataInputStream(fdis);
			blockInfo.read(new InputViewDataInputStreamWrapper(input));
			totalCount += blockInfo.getAccumulatedRecordCount();
		}

		final float avgWidth = totalCount == 0 ? 0 : ((float) stats.getTotalInputSize() / totalCount);
		return new SequentialStatistics(stats.getLastModificationTime(), stats.getTotalInputSize(), avgWidth,
			totalCount);
	}

	private static class SequentialStatistics extends FileBaseStatistics {

		private final long numberOfRecords;

		public SequentialStatistics(long fileModTime, long fileSize, float avgBytesPerRecord, long numberOfRecords) {
			super(fileModTime, fileSize, avgBytesPerRecord);
			this.numberOfRecords = numberOfRecords;
		}

		@Override
		public long getNumberOfRecords() {
			return this.numberOfRecords;
		}
	}

	@Override
	public void open(FileInputSplit split) throws IOException {
		super.open(split);

		final long blockSize = this.blockSize == NATIVE_BLOCK_SIZE ?
			this.filePath.getFileSystem().getDefaultBlockSize() : this.blockSize;

		this.blockInfo = this.createBlockInfo();
		if (this.splitLength > this.blockInfo.getInfoSize()) {
			// TODO: seek not supported by compressed streams. Will throw exception
			this.stream.seek(this.splitStart + this.splitLength - this.blockInfo.getInfoSize());
			DataInputStream infoStream = new DataInputStream(this.stream);
			this.blockInfo.read(new InputViewDataInputStreamWrapper(infoStream));
		}

		this.stream.seek(this.splitStart + this.blockInfo.getFirstRecordStart());
		BlockBasedInput blockBasedInput = new BlockBasedInput(this.stream, (int) blockSize);
		this.dataInputStream = new DataInputStream(blockBasedInput);
		this.readRecords = 0;
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return this.readRecords >= this.blockInfo.getRecordCount();
	}

	@Override
	public T nextRecord(T record) throws IOException {
		if (this.reachedEnd()) {
			return null;
		}
		
		record = this.deserialize(record, new InputViewDataInputStreamWrapper(this.dataInputStream));
		this.readRecords++;
		return record;
	}

	protected abstract T deserialize(T reuse, DataInputView dataInput) throws IOException;

	/**
	 * Writes a block info at the end of the blocks.<br>
	 * Current implementation uses only int and not long.
	 */
	protected class BlockBasedInput extends FilterInputStream {
		private final int maxPayloadSize;

		private int blockPos;

		public BlockBasedInput(FSDataInputStream in, int blockSize) {
			super(in);
			this.blockPos = (int) BinaryInputFormat.this.blockInfo.getFirstRecordStart();
			this.maxPayloadSize = blockSize - BinaryInputFormat.this.blockInfo.getInfoSize();
		}

		@Override
		public int read() throws IOException {
			if (this.blockPos++ >= this.maxPayloadSize) {
				this.skipHeader();
			}
			return this.in.read();
		}

		private void skipHeader() throws IOException {
			byte[] dummy = new byte[BinaryInputFormat.this.blockInfo.getInfoSize()];
			this.in.read(dummy, 0, dummy.length);
			this.blockPos = 0;
		}

		@Override
		public int read(byte[] b) throws IOException {
			return this.read(b, 0, b.length);
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			int totalRead = 0;
			for (int remainingLength = len, offset = off; remainingLength > 0;) {
				int blockLen = Math.min(remainingLength, this.maxPayloadSize - this.blockPos);
				int read = this.in.read(b, offset, blockLen);
				if (read < 0) {
					return read;
				}
				totalRead += read;
				this.blockPos += read;
				offset += read;
				if (this.blockPos >= this.maxPayloadSize) {
					this.skipHeader();
				}
				remainingLength -= read;
			}
			return totalRead;
		}
	}
}
