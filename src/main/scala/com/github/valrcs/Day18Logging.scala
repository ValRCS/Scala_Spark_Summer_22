package com.github.valrcs
import org.apache.log4j.Logger
import org.apache.logging.log4j.{Level}


object Day18Logging extends App {
  println(classOf[Day18Logging].getName)
  val log = Logger.getLogger(classOf[Day18Logging].getName)
  log.debug("Hello this is a debug message")
  log.info("Hello this is an info message")
}

class Day18Logging