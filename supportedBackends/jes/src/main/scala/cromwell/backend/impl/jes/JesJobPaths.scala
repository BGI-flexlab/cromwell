package cromwell.backend.impl.jes

import akka.actor.ActorSystem
import cromwell.backend.BackendJobDescriptorKey
import cromwell.backend.io.JobPaths
import cromwell.core.path.Path
import cromwell.services.metadata.CallMetadataKeys

object JesJobPaths {
  val JesLogPathKey = "jesLog"
  val GcsExecPathKey = "gcsExec"
}

case class JesJobPaths(override val workflowPaths: JesWorkflowPaths, jobKey: BackendJobDescriptorKey)(implicit actorSystem: ActorSystem) extends JobPaths {

  val jesLogBasename = {
    val index = jobKey.index.map(s => s"-$s").getOrElse("")
    s"${jobKey.scope.unqualifiedName}$index"
  }

  override val returnCodeFilename: String = s"$jesLogBasename-rc.txt"
  override val stdoutFilename: String = s"$jesLogBasename-stdout.log"
  override val stderrFilename: String = s"$jesLogBasename-stderr.log"
  override val scriptFilename: String = "exec.sh"

  val jesLogFilename: String = s"$jesLogBasename.log"
  lazy val jesLogPath: Path = callExecutionRoot.resolve(jesLogFilename)
  
  /*
  TODO: Move various monitoring files path generation here.

  "/cromwell_root" is a well known path, called in the regular JobPaths callDockerRoot.
  This JesCallPaths should know about that root, and be able to create the monitoring file paths.
  Instead of the AsyncActor creating the paths, the paths could then be shared with the CachingActor.

  Those monitoring paths could then be returned by metadataFiles and detritusFiles.
   */

  override lazy val customMetadataPaths = Map(
    CallMetadataKeys.BackendLogsPrefix + ":log" -> jesLogPath
  ) ++ (
    workflowPaths.monitoringPath map { p => Map(JesMetadataKeys.MonitoringLog -> p) } getOrElse Map.empty  
  )

  override lazy val customDetritusPaths: Map[String, Path] = Map(
    JesJobPaths.JesLogPathKey -> jesLogPath
  )

  override lazy val customLogPaths: Map[String, Path] = Map(
    JesJobPaths.JesLogPathKey -> jesLogPath
  )
}
