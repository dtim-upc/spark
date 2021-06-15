package utils

case class AttComparison(

                          var datasetName1: String ,
                          var attName1: String ,
                          var distinctValues1: Double ,

                          var datasetName2: String ,
                          var attName2: String ,
                          var distinctValues2: Double ,

                          var joinSize: Double ,
                          var containment: Double

                        )