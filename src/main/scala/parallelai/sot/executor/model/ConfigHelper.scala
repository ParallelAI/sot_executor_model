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

    //no in or out-going branches
    require(dag.findWithOutgoingEdges().isEmpty)
    require(dag.findWithIncomingEdges().isEmpty)

    //no unconnected vertices
    require(dag.unconnected == 0)

    //one sink and one source only
    require(dag.getSourceVertices().size == 1)
    require(dag.getSinkVertices().size == 1)

    dag
  }

}
