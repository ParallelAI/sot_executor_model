package parallelai.sot.executor.model

import parallelai.sot.executor.model.SOTMacroConfig.Config

object ConfigHelper {
  def validate(c: Config): Topology[String, SOTMacroConfig.DAGMapping] = validateDag(c)

  /**
   * Parse and validate DAG from config file
   */
  def validateDag(c: Config): Topology[String, SOTMacroConfig.DAGMapping] = {
    val dag = c.parseDAG()

    // No unconnected vertices
    require(dag.unconnected == 0)

    dag
  }
}