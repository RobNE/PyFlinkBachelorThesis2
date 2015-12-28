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

from flink.plan.Environment import get_environment
from flink.plan.Constants import TILE, STRING
from flink.plan.Constants import Tile
from flink.functions.FlatMapFunction import FlatMapFunction
from flink.functions.GroupReduceFunction import GroupReduceFunction
from flink.functions.KeySelectorFunction import KeySelectorFunction

class AcqDateSelector(KeySelectorFunction):
def get_key(self, value):
    return value._aquisitionDate

class SliceDetailedBlocks(FlatMapFunction):
	slicedTileWidth = detailedBlockSize
	slicedTileHeight = detailedBlockSize
	originalTileWidht, originalTileHeight
        
	def flatMap(self, iterator, collector):
		self.originalTileHeight = value.getTileHeight();
		self.originalTileWidth = value.getTileWidth();
		
		dic = dict()
	    for value in iterator:
	    	dic[value[1]] = 1
	    	
	    	
	    for key in dic.keys():
	    	collector.collect(key)

if __name__ == "__main__":
    print("found args length:", len(sys.argv))
    env = get_environment()
    if len(sys.argv) != 9:
        print("Usage: ./bin/pyflink.sh EnviCube - <dop> <input directory> <left-upper-longitude> <left-upper-latitude> <block size> <pixel size> <output path> <detailedBlockSize> ")
        sys.exit()

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

    data = env.read_envi(path, leftLong, leftLat, blockSize, pixelSize)
    pixelTimeSeries = data.group_by(AcqDateSelector(), STRING)\
        .reduce_group(CubeCreator(leftUpper, rightLower, int(blockSize), int(blockSize)), TILE)
        .flat_map(SliceDetailedBlocks(), (Tile, SlicedTile))
        .groupBy(AcqDateSelector(), STRING)\
        .reduce_group()

    env = get_environment	
		
    env.set_degree_of_parallelism(dop)

    env.execute(local=True)
