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
package org.apache.flink.examples.java.spatial;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import libsvm.svm_parameter;
import libsvm.svm_model;
import libsvm.svm_node;
import libsvm.svm_problem;
import libsvm.svm;

import org.apache.commons.lang.ArrayUtils;
//import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
//import org.apache.flink.api.common.functions.ReduceFunction;
//import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
//import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.spatial.Coordinate;
import org.apache.flink.api.java.spatial.SlicedTile;
import org.apache.flink.api.java.spatial.Tile;
import org.apache.flink.api.java.spatial.TileTimeKeySelector;
import org.apache.flink.api.java.spatial.TileTypeInformation;
import org.apache.flink.api.java.spatial.envi.TileInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
//import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;
import org.apache.commons.math.stat.regression.OLSMultipleLinearRegression;

public class SatelliteAnalysis {
	
	private static int dop;
	private static String filePath;
	private static Coordinate leftUpper, rightLower;
	private static int blockSize; // squared blocks for the beginning
	private static String outputFilePath;
	private static int pixelSize;
	private static int detailedBlockSize; //squared blocks for the beginning
	private static HashSet<Long> allDates = new HashSet<Long>(128);

	public static void main(String[] args) throws Exception {
		
		//Check if all parameters are present
		if (!parseParameters(args)) {
			return;
		}

		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();
		env.setDegreeOfParallelism(dop);
		
		//Use the readTiles function from enviCube.java to read the given scenes as Tiles 
		DataSet<Tile> tiles = readTiles(env);
		//Slice the different tiles into multiple smaller tiles. Retains only relevant data for the chosen position 
		DataSet<Tile> stitchedTimeSlices = tiles.groupBy(
				new TileTimeKeySelector<Tile>()).reduceGroup(				//Returns the acquisition date of every tile
				new TileStitchReduce().configure(leftUpper, rightLower,		//Returns all tiles which are relevant for the analysis at the given location
						blockSize, blockSize));
				
		//Slices all Tiles in smaller SlicedTiles. Then groups them by their position and sorts them by acquisitionDate. Afterwards the missing/invalid values 
		//for every group are approximated and inserted in the group.
		//TODO: Add a groupReduce to approx future values.
		DataSet<SlicedTile> slicedTiles = stitchedTimeSlices.flatMap(new SliceDetailedBlocks(detailedBlockSize, detailedBlockSize));
		/*DataSet<SlicedTile> slicedTilesSortedAndApproximated = slicedTiles
				//Group the slicedTiles by their position and the respective band
				.groupBy(new KeySelector<SlicedTile, Tuple2<Tuple2<Integer, Integer>, Integer>>() {
					private static final long serialVersionUID = 5L;
					Tuple2<Tuple2<Integer, Integer>, Integer> groupingKey = new Tuple2<Tuple2<Integer, Integer>, Integer>();

					public Tuple2<Tuple2<Integer, Integer>, Integer> getKey(SlicedTile s) {
						groupingKey.setField(s.getPositionInTile(), 0);
						groupingKey.setField(s.getBand(), 1);
						return groupingKey; 
					}
				})
                //Sort every group of SlicedTiles by their acqTime	
				.sortGroup(new SlicedTileTimeKeySelector<SlicedTile>(), Order.ASCENDING)
				//Until here everything seems to be valid. See /Users/rellerkmann/Desktop/Bachelorarbeit/Bachelorarbeit/BachelorThesis/Code/Data/out/1.txt
				.reduceGroup(new ApproxInvalidValues(detailedBlockSize, detailedBlockSize));
		*/
				
		slicedTiles.writeAsText(outputFilePath, WriteMode.OVERWRITE).setParallelism(1);
			
		env.execute("Data Cube Creation");
	}
	
	/*
	 * The sliceDetailedBlocks method returns all sliced blocks of a tile. These are smaller blocks contained in the original tile.
	 */
	
	public static final class SliceDetailedBlocks implements FlatMapFunction<Tile, SlicedTile> {
		private static final long serialVersionUID = 10L;
		//The slicedTiles' height/width in pixels
		private int slicedTileHeight;
		private int slicedTileWidth;
		//The original tiles height/width in pixels
		private int originalTileHeight;
		private int originalTileWidth;
		
		@Override
		public void flatMap(Tile value, Collector<SlicedTile> out) throws Exception {
			this.originalTileHeight = value.getTileHeight();
			this.originalTileWidth = value.getTileWidth();
			
			//System.out.println(this.originalTileHeight + " + " + this.originalTileWidth);
			
			int slicedTilesPerRow = originalTileWidth / slicedTileWidth;
			int slicedTilesPerCol = originalTileHeight / slicedTileHeight; //As long as the blocks are squared!
			short[] originalTileS16Tile = value.getS16Tile();
			
			for (int row = 0; row < slicedTilesPerRow; row++) {
				for (int col = 0; col < slicedTilesPerCol; col++) {
					//Compute the values for the slicedTile
					Coordinate slicedTileLeftUpperCoord = new Coordinate(
							Math.floor(value.getLuCord().diff(value.getRlCord()).lon / slicedTilesPerCol * row),
							Math.floor(value.getLuCord().diff(value.getRlCord()).lat / slicedTilesPerRow * col) );
					Coordinate slicedTileRightLowerCoord = new Coordinate(
							Math.floor(value.getLuCord().diff(value.getRlCord()).lon / slicedTilesPerCol * (row + 1)),
							Math.floor(value.getLuCord().diff(value.getRlCord()).lat / slicedTilesPerRow * (col + 1)) );
					int band = value.getBand();
					String acquisitionDate = value.getAqcuisitionDate();
					short[] slicedTileS16Tile = new short[0];
					
					//Cut the sliced tiles from the s16 array
					for (int slicedTileRow = 0; slicedTileRow < slicedTileHeight; slicedTileRow++) {
						//System.out.println("The slicedTileRow: " + slicedTileRow);
						//System.out.println("The original S16 size:" + originalTileS16Tile.length);
						//System.out.println("The interval beginning: " + col*slicedTileWidth + slicedTileRow*slicedTileWidth*slicedTilesPerRow + row*slicedTileWidth*slicedTileHeight*slicedTilesPerRow);
						//System.out.println("The interval end: " + col*slicedTileWidth + slicedTileRow*slicedTileWidth*slicedTilesPerRow + row*slicedTileWidth*slicedTileHeight*slicedTilesPerRow + slicedTileWidth);
						short[] tempSlicedTileS16Tile = new short[originalTileS16Tile.length];
						try {
							tempSlicedTileS16Tile = Arrays.copyOfRange(originalTileS16Tile, 
									col*slicedTileWidth + slicedTileRow*slicedTileWidth*slicedTilesPerRow + row*slicedTileWidth*slicedTileHeight*slicedTilesPerRow, 
									col*slicedTileWidth + slicedTileRow*slicedTileWidth*slicedTilesPerRow + row*slicedTileWidth*slicedTileHeight*slicedTilesPerRow + slicedTileWidth);
						} catch (Exception e) {
							System.out.println("The row:" + row);
							System.out.println("The col:" + col);
							System.out.println("The slicedTileRow:" + slicedTileRow);
							System.out.println("The slicedTilesPerRow:" + slicedTilesPerRow);
							System.out.println("The slicedTilesPerCol:" + slicedTilesPerCol);
							
							
							System.out.println("This is the error" + e.toString());
							System.out.println("The original length: " + originalTileS16Tile.length);
							
							System.out.println("The interval beginning: " + col*slicedTileWidth + slicedTileRow*slicedTileWidth*slicedTilesPerRow + row*slicedTileWidth*slicedTileHeight*slicedTilesPerRow);
							System.out.println("The interval end: " + col*slicedTileWidth + slicedTileRow*slicedTileWidth*slicedTilesPerRow + row*slicedTileWidth*slicedTileHeight*slicedTilesPerRow + slicedTileWidth);
						}
						slicedTileS16Tile = ArrayUtils.addAll(slicedTileS16Tile, tempSlicedTileS16Tile);
					}
					
					//Create the slicedTile object
					SlicedTile slicedTile = new SlicedTile(
							slicedTileLeftUpperCoord,
							slicedTileRightLowerCoord,
							//slicedTileS16Tile,
							originalTileS16Tile,
							slicedTileWidth,
							slicedTileHeight
							);
					
					slicedTile.setBand(band);
					slicedTile.setAqcuisitionDate(acquisitionDate);
					//System.out.println("The acq date: " + acquisitionDate);
					//System.out.println("The acq date of the st: " + slicedTile.getAqcuisitionDate());
					slicedTile.setPositionInTile(new Tuple2<Integer, Integer>(row, col));

					out.collect(slicedTile);
				}
			}
			
			//TODO: Check whether the different bands are handled correctly
			allDates.add(Long.parseLong(value.getAqcuisitionDate(), 10));
		}
		
		public SliceDetailedBlocks (int slicedTileWidth, int slicedTileHeight) {
			this.slicedTileHeight = slicedTileHeight;
			this.slicedTileWidth = slicedTileWidth;
		}
		
	}
	
	public static class SlicedTileTimeKeySelector<Key> implements KeySelector<SlicedTile, Long>{
		private static final long serialVersionUID = 3L;

		@Override
		public Long getKey(SlicedTile value) throws Exception {
			return Long.parseLong(value.getAqcuisitionDate(), 10);
		}
	}
	
	/*
	 * Approximates all missing values of every pixel-time-series (a group). Therefore all pixels are analyzed for every aquisitionDate. 
	 * Every instance of the groupReduce has access to every SlicedTile in a group.
	 */
	
	public static final class ApproxInvalidValues implements GroupReduceFunction<SlicedTile, SlicedTile> {
		//TODO: Import svr+ols libs, use them. Until here it is finished.
		
		private static final long serialVersionUID = 4L;
		private int slicedTileWidth;
		private int slicedTileHeight;

		@Override
		public void reduce(Iterable<SlicedTile> values, Collector<SlicedTile> out) throws Exception {
			//Create a HashMap for every pixel of the slicedTile. The HashMaps consist of the corresponding pixelTimeSeries
			System.out.println("The detailedBlockSize: " + slicedTileWidth + " " + slicedTileHeight);
			HashMap<Tuple2<Integer, Integer>, HashMap<Long, Short>> allPixelTimeSeries = new HashMap<Tuple2<Integer, Integer>, HashMap<Long, Short>>(slicedTileWidth*slicedTileHeight);
			
			//Init the array with empty pixelTimeSeries for every pixel
			for (int row = 0; row < slicedTileHeight; row++) {
				for (int col = 0; col < slicedTileWidth; col++) {
					HashMap<Long, Short> timeSeries = new HashMap<Long, Short>();
					allPixelTimeSeries.put(new Tuple2<Integer, Integer>(col, row), timeSeries);
				}
			}
			//List<Tuple2<Double[][], Double[][]>> pixelTimeSeriesSVMData = new ArrayList<Tuple2<Double[][], Double[][]>>(slicedTileWidth*slicedTileHeight);
			ArrayList<SlicedTile> slicedTiles = new ArrayList<SlicedTile>();
			//Insert the values into the pixelTimeSeries
			for (SlicedTile slicedTile : values) {
				long acquisitionDate = Long.parseLong(slicedTile.getAqcuisitionDate(), 10);
				for (int row = 0; row < slicedTileHeight; row++) {
					for (int col = 0; col < slicedTileWidth; col++) {
						short [] S16Tile = slicedTile.getSlicedTileS16Tile();
						//Get the pixel value for the pixel at position row/col
						try {
							short pixelVegetationIndex = S16Tile[row*slicedTileWidth + col];
							Tuple2<Integer, Integer> position = new Tuple2<Integer, Integer>(col, row);
							allPixelTimeSeries.get(position).put(acquisitionDate, pixelVegetationIndex);
						} catch(Exception e) {
							System.out.println("The slicedTile x: " + slicedTileWidth + " and the y: " + slicedTileHeight);
							System.out.println("The row: " + row + " and the col: " + col + " when the array fails at the position: " + (row + col));
						}
					}
				}
				slicedTiles.add(slicedTile);
			}
						
			System.out.println("The allPixelTimeSeries size: " + allPixelTimeSeries.size() + ". Should be equivalent to slicedTileHeight*slicedTileWidth = " + slicedTileHeight*slicedTileWidth);
			
			//Add the key values (the positions) to a list to make them easily accessible for the LIBSVM construction
			List<Tuple2<Integer, Integer>> allPixelTimeSeriesList = new ArrayList<Tuple2<Integer, Integer>>(allPixelTimeSeries.keySet());
			//Check the size:
			System.out.println("The size of the allPixelTimeSeriesList (should be row*col): " + allPixelTimeSeriesList.size());
			
			//Construct a LIVBSVM problem for every timePixelSeries (= for every position)
			for (Tuple2<Integer, Integer> position : allPixelTimeSeriesList) {
				List<Long>trainingSetList = new ArrayList<Long>(allPixelTimeSeries.get(position).keySet());
				int trainingSetSize = trainingSetList.size();
				double[] train_x = new double[trainingSetSize];
				svm_node[] predict_x = new svm_node[trainingSetList.size()];
				double[] train_y = new double[trainingSetSize];
				
				System.out.println("The trainingList: " + trainingSetList);
				
				for (int i=0; i < trainingSetList.size(); i++) {
					Long aqcisitionDate = trainingSetList.get(i);
					train_x [i] = aqcisitionDate.doubleValue();
					train_y [i] = allPixelTimeSeries.get(position).get(aqcisitionDate);
				}
				
				//Build the SVM_Problem
				svm_problem prob = new svm_problem();
				int countOfDates = train_x.length;
				prob.y = new double[countOfDates];
				prob.l = countOfDates;
				prob.x = new svm_node[countOfDates][];
				System.out.println("The count of dates: " + countOfDates);
				
				for (int i = 0; i < countOfDates; i++){
					double value = train_y[i];
					prob.x[i] = new svm_node[1];
					svm_node node = new svm_node();
					node.index = 0;
					node.value = train_x[i];
					prob.x[i][0] = node;
					prob.y[i] = value;
					predict_x[i] = node;
				}

				svm_parameter param = new svm_parameter();
				param.C = 1;
				param.eps = 0.1;
				param.svm_type = svm_parameter.EPSILON_SVR;
				param.kernel_type = svm_parameter.LINEAR;	
				param.probability = 1;
	
				svm_model model = svm.svm_train(prob, param);
				
				double predictedValuesSVR = svm.svm_predict(model, predict_x);
				
				System.out.println("The predicted values after SVR: " + predictedValuesSVR);
				
				double[][] olsProblem_x = new double[countOfDates][];
				double[] olsProblem_y = new double[countOfDates];
				
				for (int i = 0; i < countOfDates; i++){
					double value = train_y[i];
					olsProblem_x[i] = new double[1];
					olsProblem_x[i][0] = train_x[i];
					olsProblem_y[i] = value;
				}
				
				System.out.println("X-length: " + olsProblem_x.length);
				System.out.println("Y-length: " + olsProblem_y.length);
				
				OLSMultipleLinearRegression regressionProblem = new OLSMultipleLinearRegression();
				//regressionProblem.newSampleData(olsProblem_y, olsProblem_x);
				//regressionProblem.newSampleData(olsProblem_y, countOfDates, 0);
				regressionProblem.newSampleData(olsProblem_y, olsProblem_x);
				
				double[] predictedValuesOLS = regressionProblem.estimateResiduals();
				for (double v : predictedValuesOLS) {
					System.out.println("The predicted values after OLS: " + v);
				}
				
			}

			for (SlicedTile slicedTile : slicedTiles) {
				long acquisitionDate = Long.parseLong(slicedTile.getAqcuisitionDate(), 10);
				for (int row = 0; row < slicedTileHeight; row++) {
					for (int col = 0; col < slicedTileWidth; col++) {
						short [] S16Tile = slicedTile.getSlicedTileS16Tile();
						//Set the pixel value for the pixel at position row/col
						try {
							Tuple2<Integer, Integer> position = new Tuple2<Integer, Integer>(col, row);
							short pixelVegetationIndex = allPixelTimeSeries.get(position).get(acquisitionDate);
							S16Tile[row*slicedTileWidth + col] = pixelVegetationIndex;
							slicedTile.setSlicedTileS16Tile(S16Tile);
						} catch(Exception e) {
							System.out.println("The slicedTile x: " + slicedTileWidth + " and the y: " + slicedTileHeight);
							System.out.println("The row: " + row + " and the col: " + col + " when the array fails at the position: " + (row + col));
						}
					}
				}
				out.collect(slicedTile);
			}
		}
		
		public ApproxInvalidValues (int slicedTileWidth, int slicedTileHeight) {
			this.slicedTileHeight = slicedTileHeight;
			this.slicedTileWidth = slicedTileWidth;
		}
	}

	private static boolean parseParameters(String[] params) {

		if (params.length > 0) {
			if (params.length != 8) {
				System.out
						.println("Usage: <dop> <input directory> <left-upper-longitude> <left-upper-latitude> <block size> <pixel size> <output path> <detailedBlockSize>");
				return false;
			} else {
				dop = Integer.parseInt(params[0]);
				filePath = params[1];
				String leftLong = params[2];
				String leftLat = params[3];
				leftUpper = new Coordinate(Double.parseDouble(leftLong),
						Double.parseDouble(leftLat));

				
				blockSize = Integer.parseInt(params[4]);
				pixelSize = Integer.parseInt(params[5]);
				
				double rightLong = Double.parseDouble(leftLong) + blockSize * pixelSize;
				double rightLat = Double.parseDouble(leftLat) - blockSize * pixelSize;
				
				
				rightLower = new Coordinate(rightLong, rightLat);

				outputFilePath = params[6];
				
				//@TODO: Just for testing, add a relative size meisure later
				//detailedBlockSize = blockSize;
				detailedBlockSize = Integer.parseInt(params[7]);
			}
		} else {
			System.out
					.println("Usage: <input directory> <left-upper-longitude>  <left-upper-latitude> <block size> <pixel size> <output path> <detailedBlockSize>");
			return false;
		}

		return true;
	}

	//Read all tiles which are located in the respective path
	private static DataSet<Tile> readTiles(ExecutionEnvironment env) {
		//EnviReader enviReader = env
		//		.readEnviFile(filePath, blockSize, blockSize);
		//return enviReader.restrictTo(leftUpper, rightLower).build();
		TileInputFormat<Tile> enviFormat = new TileInputFormat<Tile>(new Path(filePath));
		enviFormat.setLimitRectangle(leftUpper, rightLower);
		enviFormat.setTileSize(blockSize, blockSize);

		return new DataSource<Tile>(env, enviFormat, new TileTypeInformation(), "enviSource");
	}

}
