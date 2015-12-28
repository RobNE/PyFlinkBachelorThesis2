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
from collections import namedtuple

class SlicedTile(namedtuple('SlicedTile', [
    'acquisitionDate', 'leftUpperLat', 'leftUpperLon',
    'rightLowerLat', 'rightLowerLon', 'width', 'height', 'band',
    'xPixelWidth', 'yPixelWidth', 'content', 'positionInTile'
    ])):

    __slots__ = ()

    @staticmethod
    def new():
        default = Tile('', '', 0.0, 0.0, 0.0, 0.0, -1, -1, -1, 0, 0, bytearray(), (0, 0))
        return default
    
    self.acquisitionDate
    self.leftUpperLat
    self.leftUpperLon
    self.rightLowerLat
    self.rightLowerLon
    self.width
    self.height
    self.band = band
    self.xPixelWidth
    self.yPixelWidth
    self.content
    self.positionInTile
    
    def update(self, leftUpper, rightLower, width, height, band,
               acquisitionDate, xPixelWidth, yPixelWidth, positionInTile):
        leftUpperLat, leftUpperLon = leftUpper
        rightLowerLat, rightLowerLon = rightLower

        self.leftUpperLat = leftUpperLat
        self.leftUpperLon = leftUpperLon
        self.rightLowerLat = rightLowerLat
        self.rightLowerLon = rightLowerLon
        self.width = width
        self.height = height
        self.band = band
        self.acquisitionDate = acquisitionDate
        self.xPixelWidth = xPixelWidth
        self.yPixelWidth = yPixelWidth
        self.positionInTile = positionInTile