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

import org.apache.commons.math.stat.regression.OLSMultipleLinearRegression;



public class OLSTest {
	public static void main(String[] args) {
		double[][] x_values = {{1.0}, {5.0}, {7.0}, {12.0}, {14.0}, {20.0} ,{21.0}};
		double[] y_values = {4., 7., 10., 8., 6., 2., 5.};
		
		OLSMultipleLinearRegression regressionProblem = new OLSMultipleLinearRegression();
		//regressionProblem.newSampleData(y_values, 7, 0);
		regressionProblem.newSampleData(y_values, x_values);
		
		double[] predictedValuesOLS = regressionProblem.estimateResiduals();
		
		double[] predictedRegressionParams = regressionProblem.estimateRegressionParameters();
		for (double v : predictedValuesOLS) {
			System.out.println("The predicted values after OLS: " + v);
		}
		
		
		for (double v : predictedRegressionParams) {
			System.out.println("The predicted regressionParams after OLS: " + v);
		}
	}
}
