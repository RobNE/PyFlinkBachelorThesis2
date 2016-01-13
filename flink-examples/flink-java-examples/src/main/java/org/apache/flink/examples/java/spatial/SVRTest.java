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

import libsvm.svm;
import libsvm.svm_node;
import libsvm.svm_parameter;
import libsvm.svm_problem;
import libsvm.svm_model;

/**
 * Created by rellerkmann on 09.01.16.
 */
public class SVRTest {
	public static void main(String[] args) {
		double[][] train_x = {{0.0}, {1.0}, {2.0}, {3.0}, {4.0}, {5.0}, {6.0}, {7.0}};
		double[] train_y = {1., 2., 4., 5., 9., 12., 15., 20.};
		svm_node[] predict_x = new svm_node[train_x.length];

		double max = train_y[0];
		for (int i = 1; i < train_y.length; i++) {
			if (train_y[i] > max) {
				max = train_y[i];
			}
		}

		double[] train_y_normalized = new double[train_y.length];
		for (int i = 0; i < train_y.length; i++) {
			train_y_normalized[i] = train_y[i] / max;
		}

		//Build the SVM_Problem
		svm_problem prob = new svm_problem();
		int countOfDates = train_x.length;
		prob.y = new double[countOfDates];
		prob.l = countOfDates;
		prob.x = new svm_node[countOfDates][];
		System.out.println("The count of dates: " + countOfDates);

		for (int i = 0; i < countOfDates; i++){
			double value = train_y_normalized[i];
			prob.x[i] = new svm_node[1];
			svm_node node = new svm_node();
			node.index = 0;
			node.value = train_x[i][0];
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
		svm_node[] nodes = new svm_node[train_y.length-1];
		for (int i = 1; i < train_y.length; i++) {
			svm_node node = new svm_node();
			node.index = i;
			node.value = train_y[i];

			nodes[i-1] = node;
		}

		int[] labels = new int[train_x.length];
		svm.svm_get_labels(model,labels);

		double[] prob_estimates = new double[train_x.length];
		double probability = svm.svm_predict_probability(model, nodes, prob_estimates);
		double[] prob_values = new double[train_x.length];
		svm.svm_predict_values(model, nodes, prob_values);

		System.out.print("The original values: ");
		for (svm_node node : nodes) {
			System.out.print(", " + node.value);
		}

		System.out.println();

		System.out.print("The predicted values after SVR: " + probability);
		System.out.print("The labels after SVR: ");
		for (int i : labels) {
			System.out.print(", " + i);
		}

		System.out.println();
		System.out.print("The prob_estimates after SVR: ");
		for (double d : prob_estimates) {
			System.out.print(", " + d);
		}

		System.out.println();

		System.out.print("The probA after SVR: ");
		for (double d : model.probA) {
			System.out.print(", " + d);
		}
	}
}
