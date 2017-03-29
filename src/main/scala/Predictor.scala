package main.scala

import org.apache.spark.SparkContext
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
  * Predict violations
  */
class Predictor(model: GradientBoostedTreesModel){//modelLocation:String, sc:SparkContext) {

//  var model: GradientBoostedTreesModel = null
//  if(modelLocation != null)
//    model = GradientBoostedTreesModel.load(sc, modelLocation)

  /**
    * Returns 1 for happy, 0 for unhappy
    * @param tweet
    * @return
    */
    def predict(tweet:String): Double ={
      if(tweet == null || tweet.length == 0)
        throw new RuntimeException("Tweet is null")
      val features = vectorize(tweet)
      return model.predict(features)
    }

    val hashingTF = new HashingTF(2000)
    def vectorize(tweet:String):Vector={
      hashingTF.transform(tweet.split(" ").toSeq)
    }

}


