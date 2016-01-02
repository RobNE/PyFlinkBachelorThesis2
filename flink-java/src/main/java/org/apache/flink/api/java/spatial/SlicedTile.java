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
package org.apache.flink.api.java.spatial;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.apache.flink.api.java.tuple.Tuple2;

public class SlicedTile implements Serializable {
	//TODO: Set this to a valid value
	private static final long serialVersionUID = 1L;
	
	private String acquisitionDate;
	private int band = -1;
	private Tuple2<Integer, Integer> positionInTile;
	private Coordinate slicedTileLeftUpperCoord;
	private Coordinate slicedTileRightLowerCoord;
	private short[] slicedTileS16Tile;
	private int slicedTileWidth;
	private int slicedTileHeight;
	
	public String getAqcuisitionDate() {
		return acquisitionDate;
	}

	public void setAqcuisitionDate(String aqcuisitionDate) {
		this.acquisitionDate = aqcuisitionDate;
	}

	public Tuple2<Integer, Integer> getPositionInTile() {
		return positionInTile;
	}

	public void setPositionInTile(Tuple2<Integer, Integer> positionInTile) {
		this.positionInTile = positionInTile;
	}

	public Tuple2<Integer, Integer> getUpperLeftPosition() {
		return upperLeftPosition;
	}

	public void setUpperLeftPosition(Tuple2<Integer, Integer> upperLeftPosition) {
		this.upperLeftPosition = upperLeftPosition;
	}

	public Tuple2<Integer, Integer> getLowerRightPosition() {
		return lowerRightPosition;
	}

	public void setLowerRightPosition(Tuple2<Integer, Integer> lowerRightPosition) {
		this.lowerRightPosition = lowerRightPosition;
	}

	public int getEdgeLength() {
		return edgeLength;
	}

	public void setEdgeLength(int edgeLength) {
		this.edgeLength = edgeLength;
	}
	
	public Long getAcquisitionDateAsLong() {
		if (this.acquisitionDate == null) {
			return new Long(-1);
		} else {
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSS"); 
			Long acquisitionDateInMilliseconds = -1L;
			
			try {
				Date date = df.parse(this.acquisitionDate);

				acquisitionDateInMilliseconds = date.getTime();  
				} catch (ParseException e) {
					//System.out.println("The parsing of the acqTime " + this.acquisitionDate + " to date failed");
			}
			
			return acquisitionDateInMilliseconds;
		}
	}

	private Tuple2<Integer, Integer> upperLeftPosition; //Tuple2<x, y>
	private Tuple2<Integer, Integer> lowerRightPosition; //Tuple2<x, y> Maybe obsolete
	private int edgeLength;
	
	public SlicedTile (String acquisitionDate, Tuple2<Integer, Integer> positionInTile, Tuple2<Integer, Integer> upperLeftPosition, int edgeLength) {
		this.acquisitionDate = acquisitionDate;
		this.positionInTile = positionInTile;
		this.upperLeftPosition = upperLeftPosition;
		this.edgeLength = edgeLength;
	}

	public SlicedTile(Coordinate slicedTileLeftUpperCoord2,
			Coordinate slicedTileRightLowerCoord2, short[] slicedTileS16Tile2,
			int slicedTileWidth2, int slicedTileHeight2) {
		this.slicedTileLeftUpperCoord = slicedTileLeftUpperCoord2;
		this.slicedTileRightLowerCoord = slicedTileRightLowerCoord2;
		this.slicedTileS16Tile = slicedTileS16Tile2;
		this.slicedTileWidth = slicedTileWidth2;
		this.slicedTileHeight = slicedTileHeight2;
	}

	//For testing only
	public SlicedTile() {
		// TODO Auto-generated constructor stub
	}

	public int getBand() {
		return band;
	}

	public void setBand(int band) {
		this.band = band;
	}

	public Coordinate getSlicedTileLeftUpperCoord() {
		return slicedTileLeftUpperCoord;
	}

	public void setSlicedTileLeftUpperCoord(Coordinate slicedTileLeftUpperCoord) {
		this.slicedTileLeftUpperCoord = slicedTileLeftUpperCoord;
	}

	public Coordinate getSlicedTileRightLowerCoord() {
		return slicedTileRightLowerCoord;
	}

	public void setSlicedTileRightLowerCoord(Coordinate slicedTileRightLowerCoord) {
		this.slicedTileRightLowerCoord = slicedTileRightLowerCoord;
	}

	public int getSlicedTileWidth() {
		return slicedTileWidth;
	}

	public void setSlicedTileWidth(int slicedTileWidth) {
		this.slicedTileWidth = slicedTileWidth;
	}

	public int getSlicedTileHeight() {
		return slicedTileHeight;
	}

	public void setSlicedTileHeight(int slicedTileHeight) {
		this.slicedTileHeight = slicedTileHeight;
	}

	public short[] getSlicedTileS16Tile() {
		return slicedTileS16Tile;
	}

	public void setSlicedTileS16Tile(short[] slicedTileS16Tile) {
		this.slicedTileS16Tile = slicedTileS16Tile;
	}

	@Override
	public String toString() {
		String newLine = System.getProperty("line.separator");
		return "SlicedTile [acquisitionDate=" + acquisitionDate + newLine
				+ ", band=" + band + newLine
				+ ", positionInTile=" + positionInTile + newLine
				+ ", slicedTileLeftUpperCoord=" + slicedTileLeftUpperCoord + newLine
				+ ", slicedTileRightLowerCoord=" + slicedTileRightLowerCoord + newLine
				+ ", slicedTileS16Tile=" + Arrays.toString(slicedTileS16Tile) + newLine
				+ ", slicedTileWidth=" + slicedTileWidth + newLine
				+ ", slicedTileHeight=" + slicedTileHeight + newLine
				+ ", upperLeftPosition=" + upperLeftPosition + newLine
				+ ", lowerRightPosition=" + lowerRightPosition + newLine
				+ ", edgeLength=" + edgeLength + "]";
	}

}
