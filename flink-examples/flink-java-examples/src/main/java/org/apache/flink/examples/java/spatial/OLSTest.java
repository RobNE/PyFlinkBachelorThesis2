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

import org.apache.commons.math.linear.RealMatrix;
import org.apache.commons.math.stat.regression.OLSMultipleLinearRegression;

public class OLSTest {
	public static void main(String[] args) {
		double[][] x_values = {{0.0}, {1.0}, {2.0}, {3.0}};
		double[] y_values = {1., 2., 4., 9.};

		double max = y_values[0];
		for (int i = 1; i < y_values.length; i++) {
			if (y_values[i] > max) {
				max = y_values[i];
			}
		}

		double[] y_values_normalized = new double[y_values.length];
		for (int i = 0; i < y_values.length; i++) {
			y_values_normalized[i] = y_values[i] / max;
		}
		
		OLSMultipleLinearRegression regressionProblem = new OLSMultipleLinearRegression();
		//regressionProblem.newSampleData(y_values, 7, 0);
		regressionProblem.newSampleData(y_values_normalized, x_values);
		
		double[] predictedValuesOLS = regressionProblem.estimateResiduals();

		/*
		for (int i=0; i < predictedValuesOLS.length; i++) {
			predictedValuesOLS[i] = predictedValuesOLS[i] * max;
		}*/
		
		double[] predictedRegressionParams = regressionProblem.estimateRegressionParameters();
		double[] estimatedRegressionParametersStandardErrors = regressionProblem.estimateRegressionParametersStandardErrors();
		RealMatrix hat = regressionProblem.calculateHat();

		System.out.print("The original values normalized: ");
		for (double v : y_values_normalized) {
			System.out.print(", " + v);
		}

		System.out.println();

		System.out.print("The predicted values after OLS: ");
		for (double v : predictedValuesOLS) {
			System.out.print(", " + v);
		}

		System.out.println();

		System.out.print("The predicted regressionParams after OLS: ");
		for (double v : predictedRegressionParams) {
			System.out.print(", " + v);
		}

		System.out.println();

		System.out.print("The predicted estimatedRegressionParametersStandardErrors after OLS: ");
		for (double v : estimatedRegressionParametersStandardErrors) {
			System.out.print(", " + v);
		}

		System.out.println();

		for (int i = 0; i < hat.getRowDimension(); i++) {
			double [] row = hat.getRow(i);
			System.out.println();
			for (double v : row) {
				System.out.print(v + ", ");
			}
		}

	}
}
