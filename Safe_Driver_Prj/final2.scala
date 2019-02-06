// Databricks notebook source
/* 
 * File uploaded to /FileStore/tables/train.csv
 * File uploaded to /FileStore/tables/sample_submission.csv
 * File uploaded to /FileStore/tables/test.csv
 */

import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.{DenseVector, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.PCA
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}




// COMMAND ----------

val Filename = "/FileStore/tables/train.csv"
val Filename2 = "/FileStore/tables/test.csv"

val data = spark.read.option("header","true"). option("inferSchema","true").option("delimiter", ",").csv(Filename)
val testData = spark.read.option("header","true"). option("inferSchema","true").option("delimiter", ",").csv(Filename2)

val labelIndexer = new StringIndexer()
  .setInputCol("target")
  .setOutputCol("label")

val data0 = labelIndexer.fit(data).transform(data)



// COMMAND ----------

val f_string="ps_ind_01,ps_ind_02_cat,ps_ind_03,ps_ind_04_cat,ps_ind_05_cat,ps_ind_06_bin,ps_ind_07_bin,ps_ind_08_bin,ps_ind_09_bin,ps_ind_10_bin,ps_ind_11_bin,ps_ind_12_bin,ps_ind_13_bin,ps_ind_14,ps_ind_15,ps_ind_16_bin,ps_ind_17_bin,ps_ind_18_bin,ps_reg_01,ps_reg_02,ps_reg_03,ps_car_01_cat,ps_car_02_cat,ps_car_03_cat,ps_car_04_cat,ps_car_05_cat,ps_car_06_cat,ps_car_07_cat,ps_car_08_cat,ps_car_09_cat,ps_car_10_cat,ps_car_11_cat,ps_car_11,ps_car_12,ps_car_13,ps_car_14,ps_car_15,ps_calc_01,ps_calc_02,ps_calc_03,ps_calc_04,ps_calc_05,ps_calc_06,ps_calc_07,ps_calc_08,ps_calc_09,ps_calc_10,ps_calc_11,ps_calc_12,ps_calc_13,ps_calc_14,ps_calc_15_bin,ps_calc_16_bin,ps_calc_17_bin,ps_calc_18_bin,ps_calc_19_bin,ps_calc_20_bin";
val f_array=f_string.split(",")

val addfeatures = new VectorAssembler()
.setInputCols(f_array)
.setOutputCol("features")


val out = addfeatures.transform(data0)
// out.show()


// COMMAND ----------


val testOut = addfeatures.transform(testData)

//val test = testOut.select("id", "features")


// COMMAND ----------

val out2 = MLUtils.convertVectorColumnsFromML(out, "features")

val testOut2 = MLUtils.convertVectorColumnsFromML(testOut, "features")


val labeled = out2.rdd.map(row => LabeledPoint(
    row.getAs[Double]("label"),
    row.getAs[Vector]("features")
))

val testLabeled = testOut2.rdd.map(row => LabeledPoint(
    row.getAs[Int]("id"),
    row.getAs[Vector]("features")
))

// COMMAND ----------

val pca = new PCA(30).fit(labeled.map(_.features))
val projected = labeled.map(p => p.copy(features = pca.transform(p.features)))
val testProjected = testLabeled.map(p => p.copy(features = pca.transform(p.features)))
val train = spark.createDataFrame(projected).toDF("label", "features")
val test = spark.createDataFrame(testProjected).toDF("id", "features")

// COMMAND ----------

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.evaluation._

import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.Row

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils

// COMMAND ----------

val lr = new LogisticRegression()

val pipeline = new Pipeline()
  .setStages(Array(lr))

val paramGrid = new ParamGridBuilder()
  .addGrid(lr.regParam, Array(0.003, 0.1, 0.05, 0.01, 0.3, 0.5))
  .addGrid(lr.maxIter, Array(5,10))
  .build()
val cv = new CrossValidator()
  .setEstimator(pipeline)
  .setEvaluator(new BinaryClassificationEvaluator)
  .setEstimatorParamMaps(paramGrid)
  .setNumFolds(5)  // Use 3+ in practice

// COMMAND ----------

val training = MLUtils.convertVectorColumnsToML(train, "features")
val testing = MLUtils.convertVectorColumnsToML(test, "features")

// COMMAND ----------

val cvModel = cv.fit(training)

// COMMAND ----------

val output = (cvModel.transform(testing)
  .select("id","prediction","probability"))

// COMMAND ----------

val o1 = output.select("id", "probability").rdd.map(r => (r(0).asInstanceOf[Double], r(1).asInstanceOf[org.apache.spark.ml.linalg.DenseVector].toArray))

// COMMAND ----------

val output = o1.map{case(id, target) => (id, target(1))}

// COMMAND ----------

display(output.toDF("id","target"))

// COMMAND ----------

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics

// COMMAND ----------

val predictions_sub = cvModel.transform(training)
val preds_sub = predictions_sub.select("probability", "label").rdd.map(row => 
  (row.getAs[Vector](0)(1), row.getAs[Double](1)))

val metrics_sub = new BinaryClassificationMetrics(preds_sub)
val auROC = metrics_sub.areaUnderROC
println("Area under ROC = " + auROC)

val auPRC = metrics_sub.areaUnderPR
println("Area under precision-recall curve = " + auPRC)
// val df_sub = predictions_sub.select("id","probability")


// COMMAND ----------

preds_sub.collect

// COMMAND ----------

// In this case the estimator is simply the linear regression.
// A TrainValidationSplit requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.

val trainValidationSplit = new TrainValidationSplit()
  .setEstimator(lr)
  .setEvaluator(new RegressionEvaluator)
  .setEstimatorParamMaps(paramGrid)
  // 80% of the data will be used for training and the remaining 20% for validation.
  .setTrainRatio(0.8)

// Run train validation split, and choose the best set of parameters.
val model = trainValidationSplit.fit(training)



// COMMAND ----------

//Make predictions on test data. model is the model with combination of parameters
// that performed best.
display(model.transform(testing)
  .select("features", "prediction"))


// COMMAND ----------

val predictions_sub1 = model.transform(training)
val preds_sub1 = predictions_sub1.select("probability", "label").rdd.map(row => 
  (row.getAs[Vector](0)(1), row.getAs[Double](1)))

val metrics_sub1 = new BinaryClassificationMetrics(preds_sub1)
val auROC1 = metrics_sub1.areaUnderROC
println("Area under ROC = " + auROC1)

val auPRC1 = metrics_sub1.areaUnderPR
println("Area under precision-recall curve = " + auPRC1)

