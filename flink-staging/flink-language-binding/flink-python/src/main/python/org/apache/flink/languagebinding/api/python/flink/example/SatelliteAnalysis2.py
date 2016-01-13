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
from struct import pack, unpack
import random
from numpy import abs, ones
from math import floor
import scipy
import matplotlib.pyplot as plt
from sklearn import svm

from flink.plan.Environment import get_environment
from flink.plan.Constants import TILE, SLICEDTILE, INT, STRING, Tile, SlicedTile, WriteMode, Order
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

class PositionSelector(KeySelectorFunction):
    def get_key(self, value):
        return (value._positionInTile[0], value._positionInTile[1], value._band)

class SliceDetailedBlocks(FlatMapFunction):
    def __init__(self,slicedTileWidth = 0, slicedTileHeight = 0):
        super(SliceDetailedBlocks, self).__init__()
        self.slicedTileWidth = slicedTileWidth
        self.slicedTileHeight = slicedTileHeight
        print("The flatmap starts")
        
    def flat_map(self, value, collector):
        originalTileHeight = value._height
        originalTileWidth = value._width
        slicedTilesPerRow = originalTileWidth / self.slicedTileWidth
        slicedTilesPerCol = originalTileHeight / self.slicedTileHeight
        originalTileS16Tile = value._content
        
        #print("The type of originalS16: " + type(originalTileS16Tile).__name__)
        
        for row in xrange (0, slicedTilesPerRow):
            for col in xrange (0, slicedTilesPerCol):
                slicedTileLeftUpperCoordLon = floor(value._leftUpperLon - value._rightLowerLon) / slicedTilesPerCol * row
                slicedTileLeftUpperCoordLat = floor(value._leftUpperLat - value._rightLowerLat) / slicedTilesPerRow * col
                
                slicedTileRightLowerCoordLon = floor((value._leftUpperLon - value._rightLowerLon) / slicedTilesPerCol * (row + 1))
                slicedTileRightLowerCoordLat = floor((value._leftUpperLat - value._rightLowerLat) / slicedTilesPerRow * (col + 1))
                
                band = value._band
                #Typo because of legacy issues
                aquisitionDate = value._aquisitionDate
                slicedTileS16Tile = bytearray()
                
                for slicedTileRow in xrange (0, self.slicedTileHeight):
                    startIndex = col*self.slicedTileWidth + slicedTileRow*self.slicedTileWidth*slicedTilesPerRow + row*self.slicedTileWidth*self.slicedTileHeight*slicedTilesPerRow
                    endIndex = col*self.slicedTileWidth + slicedTileRow*self.slicedTileWidth*slicedTilesPerRow + row*self.slicedTileWidth*self.slicedTileHeight*slicedTilesPerRow + self.slicedTileWidth
                    tempSlicedTileS16Tile = originalTileS16Tile[startIndex * 2: endIndex * 2]
                    print ("Startindex and endIndex: ", startIndex,endIndex )
                    print ("The tempSlicedTile: ",tempSlicedTileS16Tile)
                    slicedTileS16Tile.extend(tempSlicedTileS16Tile)
                    
                #print("The type of newS16: " + type(slicedTileS16Tile).__name__)
                
                slicedTile = SlicedTile()
                slicedTile._content = slicedTileS16Tile
                #slicedTile._content = originalTileS16Tile
                slicedTile._leftUpperLat = slicedTileLeftUpperCoordLat
                slicedTile._leftUpperLon = slicedTileLeftUpperCoordLon
                slicedTile._rightLowerLat = slicedTileRightLowerCoordLat
                slicedTile._rightLowerLon = slicedTileRightLowerCoordLon
                slicedTile._width = self.slicedTileWidth
                slicedTile._height = self.slicedTileHeight
                slicedTile._band = band
                slicedTile._aquisitionDate = aquisitionDate
                slicedTile._positionInTile = (row, col)

                print ("The aqu Date: " + str(slicedTile._aquisitionDate))
                
                collector.collect(slicedTile)
                
        #self.allDatesList.append(value._aquisitionDate)
        print("The flatmap is over")
        
class ApproxInvalidValues(GroupReduceFunction):
    def reduce(self, iterator, collector):
        allPixelTimeSeries = {} #the dic containing all pixelTimeSeries
        pixelTimeSeries = {} #a pixelTimeSeries
        slicedTiles = []
        allDatesList = set()
        
        for row in xrange (0, self.slicedTileHeight):
            for col in range (0, self.slicedTileWidth):
                positionInTile = row, col
                allPixelTimeSeries[positionInTile] = pixelTimeSeries
        
        for slicedTile in iterator:
            acquisitionDate = slicedTile._aquisitionDate
            allDatesList.add(acquisitionDate)
            for row in range (0, self.slicedTileHeight):
                for col in range (0, self.slicedTileWidth):
                    S16Tile = slicedTile._content

                    #TODO: Check whether the assignment of the arrays works as expected
                    index = (row*self.slicedTileWidth+col)*2
                    pixelVegetationIndexBytes = S16Tile[index: index+2]
                    pixelVegetationIndex = unpack("<h", pixelVegetationIndexBytes)[0]
                    print ("The pxVegIndex after unpack: " + str(pixelVegetationIndex))
                    currentPixelTimeSeries = allPixelTimeSeries[(row, col)]
                    currentPixelTimeSeries[acquisitionDate] = pixelVegetationIndex

            slicedTiles.append(slicedTile)

        print ("The count of slicedTileObjects: ") + str(len(slicedTiles))
        trainingSetSize = len(allDatesList)
        print ("The trainingSetSize: " + str(trainingSetSize))
        
        for position in allPixelTimeSeries:
            train_x = []
            train_y = []
            
            currentPixelTimeSeries = allPixelTimeSeries[position]
            currentPixelTimeSeriesKeys = currentPixelTimeSeries.keys()
            
            trainingSetList = currentPixelTimeSeriesKeys
            trainingSetList.sort()
            
            for i in xrange (0, len(trainingSetList)):
                acquisitionDate = trainingSetList[i]
                pixelTimeSeriesValues = allPixelTimeSeries[position]
                if (pixelTimeSeriesValues[acquisitionDate] > -9999 and pixelTimeSeriesValues[acquisitionDate] < 16000):
                    train_x.append(acquisitionDate)
                    train_y.append(pixelTimeSeriesValues[acquisitionDate])

            print ("Train_y before fit: " + str(train_y))
            print ("Train_x: before fit" + str(train_x))
            
            #Create the SVM Problem
            if ((len(train_x) > 1) and (len(train_y) > 1)):
                try:
                    svr = svm.SVR(gamma=1./(2.*(3/12.)**2), C=1, epsilon=0.1)
                    svr.fit([[x] for x in train_x], train_y)
                    #svr.predict([[x] for x in train_x])
            
                    def f(x, m, n):
                        print ("x" + str(x))
                        print ("m" + str(m))
                        print ("n" + str(n))
                        return m*x+n

                    #print ("Train_y: " + str(train_y))
                    #print ("Train_x: " + str(train_x))

                    fitpars, covmat = scipy.optimize.curve_fit(f, [int(x) for x in train_x], train_y)
                    print('observed:', train_y)
                    print('predicted:', str(f(train_x, *fitpars)))
                except TypeError, e:
                    print ("The analysis failed due to TypeError e:", e)

            else:
                print("The parameters were not sufficient to execute the analysis")
            
        for slicedTile in slicedTiles:
            aquisitionDate = slicedTile._aquisitionDate
            S16Tile = slicedTile._content
            for row in xrange (0, self.slicedTileHeight):
                for col in xrange (0, self.slicedTileWidth):
                    position = row, col
                    pixelVegetationIndex = allPixelTimeSeries.get(position).get(aquisitionDate)
                    print ("The pxVegIndex as Number: " + str(pixelVegetationIndex))
                    pixelVegetationIndexBytes = pack("<h", pixelVegetationIndex)
                    print ("The pxVegIndex after packing as bytes: " + str(pixelVegetationIndexBytes))
                    S16Tile[row*self.slicedTileWidth + col] = pixelVegetationIndexBytes[0]
                    S16Tile[row*self.slicedTileWidth + col + 1] = pixelVegetationIndexBytes[1]
                    #print ("The pxVegIndex after packing: " + str(S16Tile[row*self.slicedTileWidth + col: row*self.slicedTileWidth + col + 2]))
            slicedTile._content = S16Tile
            collector.collect(slicedTile)
        
    def __init__(self, slicedTileWidth = 0, slicedTileHeight = 0):
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
    
    output_file = "file:///Users/rellerkmann/Desktop/Bachelorarbeit/Bachelorarbeit/BachelorThesis/Code/Data/outPython/pythonCuttingWithSlicedS16.txt"

    data = env.read_envi(path, leftLong, leftLat, blockSize, pixelSize)
    pixelTimeSeries = data.group_by(AcqDateSelector(), STRING)\
        .reduce_group(CubeCreator(leftUpper, rightLower, int(blockSize), int(blockSize)), TILE)\
        .flat_map(SliceDetailedBlocks(int(detailedBlockSize), int(detailedBlockSize)), SLICEDTILE)\
        .group_by(PositionSelector(), (INT, INT, INT)) \
        .reduce_group(ApproxInvalidValues(int(detailedBlockSize), int(detailedBlockSize)), SLICEDTILE)\
        .write_text(output_file, write_mode=WriteMode.OVERWRITE)
        #.group_by(AcqDateSelector(), STRING)\
        #.sort_group(AcqDateSelector(), Order.ASCENDING)\
        #.reduce_group(ApproxInvalidValues(detailedBlockSize, detailedBlockSize), SLICEDTILE)\
        #.write_text(output_file, write_mode=WriteMode.OVERWRITE)
        #.group_by(AcqDateSelector(), STRING)\
        #.sort_group(AcqDateSelector(), STRING)\
        
        #.group_by(AcqDateSelector(), STRING)\
        #.sort_group(AcqDateSelector(), STRING)\
        #.reduce_group(ApproxInvalidValues(detailedBlockSize, detailedBlockSize), SLICEDTILE)\
        
    print("detailedBlockSize: ", detailedBlockSize)
    env.set_degree_of_parallelism(dop)

    env.execute(local=True)
