package org.apache.flink.client.cli
import org.apache.commons.cli.CommandLine

class StopOptions(line: CommandLine) extends CommandLineOptions(line: CommandLine) {

  private val args: Array[String] = line.getArgs

  private val savepointFlag: Boolean = line.hasOption(CliFrontendParser.STOP_WITH_SAVEPOINT_PATH.getOpt)

  /** Optional target directory for the savepoint. Overwrites cluster default. */
  private val targetDirectory = line.getOptionValue(CliFrontendParser.STOP_WITH_SAVEPOINT_PATH.getOpt)

  private val advanceToEndOfEventTime: Boolean = line.hasOption(CliFrontendParser.STOP_AND_DRAIN.getOpt)

  def getArgs() = {
    if (args == null) {
      new Array[String](0)
    } else {
      args
    }
  }

  def hasSavepointFlag() = {
    savepointFlag
  }

  def getTargetDirectory() = {
    targetDirectory
  }

  def shouldAdvanceToEndOfEventTime() = {
    advanceToEndOfEventTime
  }
}
