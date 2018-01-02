package parallelai.sot.executor.model

import parallelai.sot.executor.model.SOTMacroConfig.Config


object ConfigHelper {

  def validate(c: Config) = {
    validateDag(c)
  }

  /**
    * Parse and validate DAG from config file
    */
  def validateDag(c: Config) = {

    val dag = c.parseDAG()

    //no unconnected vertices
    require(dag.unconnected == 0)

    dag
  }

}
