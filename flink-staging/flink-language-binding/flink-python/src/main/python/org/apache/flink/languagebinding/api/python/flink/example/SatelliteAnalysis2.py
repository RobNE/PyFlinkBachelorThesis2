################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import sys

from collections import defaultdict
from struct import pack
import random
from numpy import abs, ones
import scipy
import matplotlib.pyplot as plt
from sklearn import svm

from flink.plan.Environment import get_environment
from flink.plan.Constants import TILE, SLICEDTILE, INT, STRING, Tile, SlicedTile, WriteMode
from flink.functions.FlatMapFunction import FlatMapFunction
from flink.functions.GroupReduceFunction import GroupReduceFunction
from flink.functions.KeySelectorFunction import KeySelectorFunction

NOVAL = pack("<h", -9999)

class CubeCreator(GroupReduceFunction):
    def __init__(self, leftUpper=(0, 0), rightLower=(0, 0), xSize=0, ySize=0):
        super(CubeCreator, self).__init__()
        self.leftUpperLat, self.leftUpperLon = leftUpper
        self.rightLowerLat, self.rightLowerLon = rightLower
        self.xSize = xSize
        self.ySize = ySize

    @property
    def leftUpper(self):
        return (self.leftUpperLat, self.leftUpperLon)

    @property
    def rightLower(self):
        return (self.rightLowerLat, self.rightLowerLon)

    def reduce(self, iterator, collector):
        """
        :param iterator:
        :param collector:
        :return:
        """
        # group tiles by band
        band_to_tiles = defaultdict(set)
        for tile in iterator:
            band_to_tiles[tile._band].add(tile)

        # iterate over bands in order
        bands = sorted(band_to_tiles.keys())
        orig_not_null_counter = 0
        inside_counter = 0
        known_counter = 0
        for b in bands:
            result = Tile()
            # Initialize content with -9999
            result._content = bytearray(self.xSize * self.ySize * 2)
            for i in range(0, len(result._content), 2):
                result._content[i] = NOVAL[0]
                result._content[i+1] = NOVAL[1]

            # iterate over tiles for current band
            updated = False
            for t in band_to_tiles[b]:
                if not updated:
                    result.update(self.leftUpper, self.rightLower, self.xSize,
                                  self.ySize, b, t._pathRow, t._aquisitionDate,
                                  t._xPixelWidth, t._yPixelWidth)
                    updated = True

                for i, (px_coord_lat, px_coord_lon) in coord_iter(t):
                    if t._content[i:i+2] != NOVAL:
                        orig_not_null_counter += 1

                    if (self.leftUpperLat >= px_coord_lat and
                            px_coord_lat >= self.rightLowerLat and
                            self.leftUpperLon <= px_coord_lon and
                            px_coord_lon <= self.rightLowerLon):
                        # get index in result tile for current pixel
                        index = int(result.get_content_index_from_coordinate((px_coord_lat, px_coord_lon)))
                        if index >= 0 and index < len(result._content):
                            inside_counter += 1
                            px_value = t._content[i:i+2]
                            if px_value != NOVAL:
                                known_counter += 1
                            result._content[index] = px_value[0]
                            result._content[index+1] = px_value[1]

            collector.collect(result)

        print("inside", inside_counter)
        print("known_counter", known_counter)
        print("orig not null", orig_not_null_counter)
        
def coord_iter(tile):
    lon = tile._leftUpperLon
    lat = tile._leftUpperLat
    yield (0, (lat, lon))
    if len(tile._content) > 2:
        for i in range(2, len(tile._content), 2):
            if i % tile._width == 0:
                lon = tile._leftUpperLon
                lat -= tile._yPixelWidth
            else:
                lon += tile._xPixelWidth
            yield (i, (lat, lon))

class AcqDateSelector(KeySelectorFunction):
    def get_key(self, value):
        return value._aquisitionDate

class SliceDetailedBlocks(FlatMapFunction):
    #slicedTileWidth, slicedTileHeight, originalTileWidht, originalTileHeight
    
    def __init__(self,slicedTileWidth, slicedTileHeight):
        super(SliceDetailedBlocks, self).__init__()
        self.slicedTileWidth = slicedTileWidth
        self.slicedTileHeight = slicedTileHeight
        
    def flatMap(self, iterator, collector):
        print("The flatmap starts")
        self.originalTileHeight = value.getTileHeight()
        self.originalTileWidth = value.getTileWidth()
        slicedTilesPerRow = self.originalTileWidth / self.slicedTileWidth
        slicedTilesPerCol = self.originalTileHeight / self.slicedTileHeight
        originalTileS16Tile = value._content
        
        
        for row in range (0, slicedTilesPerRow):
            for col in range (0, slicedTilesPerCol):
                slicedTileLeftUpperCoordLon = Math.floor((value._leftUpperLon - value._rightLowerLon) / slicedTilesPerCol * row)
                slicedTileLeftUpperCoordLat = Math.floor((value._leftUpperLat - value._rightLowerLat) / slicedTilesPerRow * col)
                
                slicedTileRightLowerCoordLon = Math.floor((value._leftUpperLon - value._rightLowerLon) / slicedTilesPerCol * (row + 1))
                slicedTileRightLowerCoordLat = Math.floor((value._leftUpperLat - value._rightLowerLat) / slicedTilesPerRow * (col + 1))
                
        
                band = value._band
                acquisitionDate = value._acquisitionDate
                slicedTileS16Tile
                
                for slicedTileRow in range (0, self.slicedTileHeight):
                    tempSlicedTileS16Tile = originalTileS16Tile[col*slicedTileWidth + slicedTileRow*slicedTileWidth*slicedTilesPerRow + row*slicedTileWidth*slicedTileHeight*slicedTilesPerRow:col*slicedTileWidth + slicedTileRow*slicedTileWidth*slicedTilesPerRow + row*slicedTileWidth*slicedTileHeight*slicedTilesPerRow + slicedTileWidth]
                    slicedTileS16Tile.append(tempSlicedTileS16Tile) 
                
                slicedTile = SlicedTile()
                slicedTile._content = slicedTileS16Tile
                slicedTile._leftUpperLat = slicedTileLeftUpperCoordLat
                slicedTile._leftUpperLon = slicedTileLeftUpperCoordLon
                slicedTile._rightLowerLat = slicedTileRightLowerCoordLat
                slicedTile._rightLowerLon = slicedTileRightLowerCoordLon
                slicedTile._xPixelWidth = self.slicedTileWidth
                slicedTile._yPixelWidth = self.slicedTileHeight
                slicedTile._band = band
                slicedTile._acquisitionDate = acquisitionDate
                slicedTile._positionInTile = (row, col)
                
                collector.collect(slicedTile)
                
        self.allDatesList.append(value._aquisitionDate)
        print("The flatmap is over")
        
class ApproxInvalidValues(GroupReduceFunction):
    def reduce(self, iterator, collector):
        allPixelTimeSeries = {} #the dic containing all pixelTimeSeries
        pixelTimeSeries = {} #a pixelTimeSeries
        positionInTile #the position of a pixelTimeSeries (funcs as the key for the main dic)
        slicedTiles = []
        
        for row in range (0, slicedTileHeight):
            for col in range (0, slicedTileWidth):
                positionInTile = row, col
                allPixelTimeSeries[positionInTile] = pixelTimeSeries
        
        for slicedTile in iterator:
            acquisitionDate = slicedTile._acquisitionDate
            for row in range (0, slicedTileHeight):
                for col in range (0, slicedTileWidth):
                    S16Tile = slicedTile._content
                    try:
                        #TODO: Check whether the assignment of the arrays works as expected
                        pixelVegetationIndex = S16Tile[row*slicedTileWidth+col]
                        currentPixelTimeSeries = allPixelTimeSeries[(row, col)]
                        currentPixelTimeSeries[acquisitionDate] = pixelVegetationIndex
                    except:
                        print("the vegIndex gave an error")
            slicedTiles.append(slicedTile)
        
        trainingSetSize = self.allDatesList.length
        
        for position in allPixelTimeSeries:
            train_x, train_y
            
            currentPixelTimeSeries = allPixelTimeSeries[position]
            currentPixelTimeSeriesKeys = currentPixelTimeSeries.keys()
            
            trainingSetList = currentPixelTimeSeriesKeys
            
            for i in range (0, trainingSetList.length):
                acquisitionDate = trainingSetList[i]
                train_x [i] = aquisitionDate
                pixelTimeSeriesValues = allPixelTimeSeries[position]
                train_y [i] = pixelTimeSeriesValues[acquisitionDate]
            
            #Create the SVM Problem
            svr = svm.SVR(gamma=1./(2.*(3/12.)**2), C=1, epsilon=0.1)
            svr.fit(train_x.reshape([-1,1]), train_y, sample_weight=None)
            
            def f(x, m, n):
                return m*x+n
            
            fitpars, covmat = scipy.optimize.curve_fit(f, x.flatten(), y)
            print('observed:', y)
            print('predicted:', f(x.flatten(), *fitpars))
            
            
        for slicedTile in slicedTiles:
            aquisitionDate = slicedTile._acquisitionDate
            for row in range (0, self.slicedTileHeight):
                for col in range (0, self.slicedTileWidth):
                    S16Tile = slicedTile._content
                    position = row, col
                    pixelVegetationIndex = allPixelTimeSeries.get(position).get(aquisitionDate)
                    S16tile[row*self.slicedTileWidth + col] = pixelVegetationIndex
                    slicedTile._content = S16Tile
            out.collect(slicedTile)
        
    def __init__(self,slicedTileWidth, slicedTileHeight):
        super(ApproxInvalidValues, self).__init__()
        self.slicedTileWidth = slicedTileWidth
        self.slicedTileHeight = slicedTileHeight 
        

if __name__ == "__main__":
    print("found args length:", len(sys.argv))
    env = get_environment()
    if len(sys.argv) != 9:
        print("Usage: ./bin/pyflink.sh EnviCube - <dop> <input directory> <left-upper-longitude> <left-upper-latitude> <block size> <pixel size> <output path> <detailedBlockSize> ")
        sys.exit()
        
    allDatesList = [] 

    dop = int(sys.argv[1])
    path = sys.argv[2]
    leftLong = sys.argv[3]
    leftLat = sys.argv[4]
    
    blockSize = sys.argv[5]
    pixelSize = sys.argv[6]
    outputPath = sys.argv[7]
    detailedBlockSize = sys.argv[8]

    leftUpper = (float(leftLat), float(leftLong))
    rightLower = (float(leftLat) - int(blockSize) * int(pixelSize),
                  float(leftLong) + int(blockSize) * int(pixelSize))
    
    output_file = "file:///Users/rellerkmann/Desktop/Bachelorarbeit/Bachelorarbeit/BachelorThesis/Code/Data/out/2.txt"

    data = env.read_envi(path, leftLong, leftLat, blockSize, pixelSize)
    pixelTimeSeries = data.group_by(AcqDateSelector(), STRING)\
        .reduce_group(CubeCreator(leftUpper, rightLower, int(blockSize), int(blockSize)), TILE)\
        .flat_map(SliceDetailedBlocks(detailedBlockSize, detailedBlockSize), SLICEDTILE)\
        .group_by(AcqDateSelector(), STRING)\
        .sort_group(AcqDateSelector(), Order.ASCENDING)\
        .reduce_group(ApproxInvalidValues(detailedBlockSize, detailedBlockSize), SLICEDTILE)\
        .write_text(output_file, write_mode=WriteMode.OVERWRITE)
        #.group_by(AcqDateSelector(), STRING)\
        #.sort_group(AcqDateSelector(), STRING)\
        
        #.group_by(AcqDateSelector(), STRING)\
        #.sort_group(AcqDateSelector(), STRING)\
        #.reduce_group(ApproxInvalidValues(detailedBlockSize, detailedBlockSize), SLICEDTILE)\
        
    print("dop: ", dop)
    env.set_degree_of_parallelism(dop)

    env.execute(local=True)
