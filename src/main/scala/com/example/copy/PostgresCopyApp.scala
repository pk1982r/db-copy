package com.example.copy

import cats.effect.*
import cats.effect.std.Console
import cats.implicits.*

import java.sql.{BatchUpdateException, Connection, DriverManager}
import scala.concurrent.duration.*

/** Simple application for copying data from one PostgreSQL database to another.
 *
 * This tool performs a straightforward table copy with optional throttling.
 * It reads data from a source query and inserts it into a target table.
 *
 * Environment variables required:
 *   - SOURCE_DB_HOST: Source database host
 *   - SOURCE_DB_PORT: Source database port
 *   - SOURCE_DB_NAME: Source database name
 *   - SOURCE_DB_USER: Source database username
 *   - SOURCE_DB_PASSWORD: Source database password
 *   - SOURCE_DB_SCHEMA: (optional) Source database schema (default: public)
 *   - TARGET_DB_HOST: Target database host
 *   - TARGET_DB_PORT: Target database port
 *   - TARGET_DB_NAME: Target database name
 *   - TARGET_DB_USER: Target database username
 *   - TARGET_DB_PASSWORD: Target database password
 *   - TARGET_DB_SCHEMA: (optional) Target database schema (default: public)
 *   - SOURCE_QUERY: SQL query to select data (e.g., "SELECT * FROM table_name" or "SELECT * FROM schema.table WHERE ...")
 *   - TARGET_TABLE: Table to insert into (e.g., "table_name" - will use TARGET_DB_SCHEMA)
 *   - TARGET_COLUMNS: (optional) Comma-separated list of target columns if different from source (e.g., "col1,col2,col3")
 *   - BATCH_SIZE: (optional) Number of rows per batch (default: 1000)
 *   - THROTTLE_DELAY_MS: (optional) Delay between batches in milliseconds (default: 0)
 *
 * Example usage:
 * {{{
 * export SOURCE_DB_HOST=localhost
 * export SOURCE_DB_PORT=5432
 * export SOURCE_DB_NAME=source_db
 * export SOURCE_DB_USER=postgres
 * export SOURCE_DB_PASSWORD=password
 * export SOURCE_DB_SCHEMA=my_schema
 * export TARGET_DB_HOST=localhost
 * export TARGET_DB_PORT=5433
 * export TARGET_DB_NAME=target_db
 * export TARGET_DB_USER=postgres
 * export TARGET_DB_PASSWORD=password
 * export TARGET_DB_SCHEMA=another_schema
 * export SOURCE_QUERY="SELECT id, name, email FROM users WHERE active = true"
 * export TARGET_TABLE="users"
 * export TARGET_COLUMNS="user_id,full_name,email_address"  # Optional: map source columns to different target columns
 * export BATCH_SIZE=500
 * export THROTTLE_DELAY_MS=100
 *
 * sbt "runMain com.swissborg.controlling.service.tools.PostgresCopyApp"
 * }}}
 */

// Following class was vibe coded.
object PostgresCopyApp extends IOApp.Simple {

  final case class DbConfig(
                             host: String,
                             port: Int,
                             database: String,
                             user: String,
                             password: String,
                             schema: String,
                           ) {
    def connectionString: String = s"$user@$host:$port/$database (schema: $schema)"

    def jdbcUrl: String = s"jdbc:postgresql://$host:$port/$database?currentSchema=$schema"
  }

  final case class CopyConfig(
                               sourceQuery: String,
                               targetTable: String,
                               targetColumns: Option[List[String]],
                               batchSize: Int,
                               throttleDelay: FiniteDuration,
                             )

  /** Read database configuration from environment variables */
  def readDbConfig(prefix: String): IO[DbConfig] =
    for {
      host <- IO(sys.env(s"${prefix}_DB_HOST"))
      port <- IO(sys.env(s"${prefix}_DB_PORT").toInt)
      database <- IO(sys.env(s"${prefix}_DB_NAME"))
      user <- IO(sys.env(s"${prefix}_DB_USER"))
      password <- IO(sys.env(s"${prefix}_DB_PASSWORD"))
      schema <- IO(sys.env.getOrElse(s"${prefix}_DB_SCHEMA", "public"))
    } yield DbConfig(host, port, database, user, password, schema)

  /** Read copy configuration from environment variables with defaults */
  def readCopyConfig: IO[CopyConfig] =
    for {
      sourceQuery <- IO(sys.env("SOURCE_QUERY"))
      targetTable <- IO(sys.env("TARGET_TABLE"))
      targetColumnsStr <- IO(sys.env.get("TARGET_COLUMNS"))
      targetColumns = targetColumnsStr.map(_.split(",").map(_.trim).toList) // TODO study this
      batchSize <- IO(sys.env.getOrElse("BATCH_SIZE", "1000").toInt)
      throttleDelayMs <- IO(sys.env.getOrElse("THROTTLE_DELAY_MS", "0").toLong)
    } yield CopyConfig(sourceQuery, targetTable, targetColumns, batchSize, throttleDelayMs.millis)

  /** Create a JDBC connection resource with schema set */
  def createConnection(config: DbConfig): Resource[IO, Connection] =
    Resource.make(
      IO {
        Class.forName("org.postgresql.Driver")
        val conn = DriverManager.getConnection(config.jdbcUrl, config.user, config.password)
        // Explicitly set the search path to the specified schema
        val stmt = conn.createStatement()
        stmt.execute(s"SET search_path TO ${config.schema}")
        stmt.close()
        conn
      }
    )(conn => IO(conn.close()))

  /** Parse table name to handle schema qualification */
  def parseTableName(tableName: String, defaultSchema: String): (String, String) = {
    tableName.split("\\.").toList match {
      case schema :: table :: Nil => (schema, table)
      case table :: Nil => (defaultSchema, table)
      case _ => throw new IllegalArgumentException(s"Invalid table name: $tableName")
    }
  }

  /** Get fully qualified table name */
  def getFullyQualifiedTable(tableName: String, schema: String): String = {
    val (tableSchema, table) = parseTableName(tableName, schema)
    s"$tableSchema.$table"
  }

  /** Validate that schema exists */
  def validateSchema(conn: Connection, schema: String): IO[Unit] = IO {
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(
      s"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '$schema'"
    )
    val exists = rs.next()
    rs.close()
    stmt.close()

    if (!exists) {
      throw new IllegalArgumentException(s"Schema '$schema' does not exist in the database")
    }
  }

  /** Get target table columns */
  def getTargetTableColumns(conn: Connection, tableName: String, schema: String): IO[List[String]] = IO {
    val (tableSchema, table) = parseTableName(tableName, schema)
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(
      s"""
         |SELECT column_name
         |FROM information_schema.columns
         |WHERE table_schema = '$tableSchema'
         |AND table_name = '$table'
         |ORDER BY ordinal_position
       """.stripMargin
    )

    val columns = scala.collection.mutable.ListBuffer[String]()
    while (rs.next()) {
      columns += rs.getString("column_name")
    }
    rs.close()
    stmt.close()

    if (columns.isEmpty) {
      throw new IllegalArgumentException(s"Table '$table' does not exist in schema '$tableSchema' or has no columns")
    }

    columns.toList
  }

  /** Validate that table exists in the specified schema */
  def validateTable(conn: Connection, tableName: String, schema: String): IO[Unit] = IO {
    val (tableSchema, table) = parseTableName(tableName, schema)
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(
      s"""
         |SELECT table_name
         |FROM information_schema.tables
         |WHERE table_schema = '$tableSchema'
         |AND table_name = '$table'
       """.stripMargin
    )
    val exists = rs.next()
    rs.close()
    stmt.close()

    if (!exists) {
      throw new IllegalArgumentException(s"Table '$table' does not exist in schema '$tableSchema'")
    }
  }

  /** Validate column mapping */
  def validateColumnMapping(
                             sourceColumns: List[String],
                             targetColumns: List[String],
                             availableTargetColumns: List[String]
                           ): IO[Unit] = IO {
    if (sourceColumns.size != targetColumns.size) {
      throw new IllegalArgumentException(
        s"Column count mismatch: source has ${sourceColumns.size} columns but target has ${targetColumns.size} columns"
      )
    }

    val missingColumns = targetColumns.filterNot(availableTargetColumns.contains)
    if (missingColumns.nonEmpty) {
      throw new IllegalArgumentException(
        s"Target columns do not exist: ${missingColumns.mkString(", ")}\n" +
          s"Available columns: ${availableTargetColumns.mkString(", ")}"
      )
    }
  }

  /** Get column information from source query */
  def getSourceColumns(query: String, conn: Connection): IO[List[String]] = IO {
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(s"$query LIMIT 0")
    val meta = rs.getMetaData
    val columnCount = meta.getColumnCount
    val columns = (1 to columnCount).map(meta.getColumnName).toList
    rs.close()
    stmt.close()
    columns
  }

  /** Count total rows in source query */
  def countRows(query: String, conn: Connection): IO[Long] = IO {
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(s"SELECT COUNT(*) FROM ($query) AS count_query")
    rs.next()
    val count = rs.getLong(1)
    rs.close()
    stmt.close()
    count
  }

  /** Copy data using JDBC batch operations with better error handling */
  def copyData(
                config: CopyConfig,
                targetColumns: List[String],
                sourceConn: Connection,
                targetConn: Connection,
                targetSchema: String,
                totalRows: Long,
              ): IO[Long] = IO {
    val columnList = targetColumns.mkString(", ")
    val placeholders = targetColumns.map(_ => "?").mkString(", ")
    val fullyQualifiedTarget = getFullyQualifiedTable(config.targetTable, targetSchema)
    val insertSql = s"INSERT INTO $fullyQualifiedTarget ($columnList) VALUES ($placeholders)"

    val sourceStmt = sourceConn.createStatement()
    sourceStmt.setFetchSize(config.batchSize)
    val rs = sourceStmt.executeQuery(config.sourceQuery)
    val targetStmt = targetConn.prepareStatement(insertSql)

    var totalCopied = 0L
    var batchCount = 0
    var batchIndex = 0

    try {
      while (rs.next()) {
        // Set parameters from result set
        for (i <- 1 to targetColumns.size) {
          targetStmt.setObject(i, rs.getObject(i))
        }
        targetStmt.addBatch()
        batchCount += 1

        // Execute batch when full
        if (batchCount >= config.batchSize) {
          try {
            val results = targetStmt.executeBatch()
            val inserted = results.sum
            totalCopied += inserted
            batchIndex += 1
            val processed = (batchIndex * config.batchSize).toLong.min(totalRows)
            val progress = if (totalRows > 0) (processed * 100.0 / totalRows).toInt else 100

            println(f"  Batch $batchIndex%4d: $inserted%5d rows | Progress: $processed%8d/$totalRows%8d ($progress%3d%%)")
          } catch {
            case e: BatchUpdateException =>
              println(s"\n❌ Error in batch $batchIndex:")
              println(s"   SQL: $insertSql")
              println(s"   Columns: ${targetColumns.mkString(", ")}")
              println(s"   Error: ${e.getMessage}")
              var nextEx = e.getNextException
              while (nextEx != null) {
                println(s"   Next error: ${nextEx.getMessage}")
                nextEx = nextEx.getNextException
              }
              throw e
          }

          targetStmt.clearBatch()
          batchCount = 0

          // Throttle if configured
          if (config.throttleDelay.toMillis > 0) {
            Thread.sleep(config.throttleDelay.toMillis)
          }
        }
      }

      // Execute remaining batch
      if (batchCount > 0) {
        try {
          val results = targetStmt.executeBatch()
          val inserted = results.sum
          totalCopied += inserted
          batchIndex += 1
          val progress = 100
          println(f"  Batch $batchIndex%4d: $inserted%5d rows | Progress: $totalCopied%8d/$totalRows%8d ($progress%3d%%)")
        } catch {
          case e: BatchUpdateException =>
            println(s"\n❌ Error in final batch:")
            println(s"   SQL: $insertSql")
            println(s"   Columns: ${targetColumns.mkString(", ")}")
            println(s"   Error: ${e.getMessage}")
            var nextEx = e.getNextException
            while (nextEx != null) {
              println(s"   Next error: ${nextEx.getMessage}")
              nextEx = nextEx.getNextException
            }
            throw e
        }
      }

      totalCopied
    } finally {
      rs.close()
      sourceStmt.close()
      targetStmt.close()
    }
  }

  /** Main copy operation with progress tracking */
  def performCopy(
                   config: CopyConfig,
                   sourceConfig: DbConfig,
                   targetConfig: DbConfig,
                 ): IO[Long] =
    for {
      _ <- Console[IO].println("📋 Validating configuration...")

      result <- (createConnection(sourceConfig), createConnection(targetConfig)).tupled.use {
        case (sourceConn, targetConn) =>
          for {
            // Validate schemas
            _ <- validateSchema(sourceConn, sourceConfig.schema)
            _ <- Console[IO].println(s"✓ Source schema '${sourceConfig.schema}' validated")
            _ <- validateSchema(targetConn, targetConfig.schema)
            _ <- Console[IO].println(s"✓ Target schema '${targetConfig.schema}' validated")

            // Validate target table and get its columns
            _ <- validateTable(targetConn, config.targetTable, targetConfig.schema)
            fullTargetName = getFullyQualifiedTable(config.targetTable, targetConfig.schema)
            availableTargetColumns <- getTargetTableColumns(targetConn, config.targetTable, targetConfig.schema)
            _ <- Console[IO].println(s"✓ Target table '$fullTargetName' validated")
            _ <- Console[IO].println(s"   Available target columns: ${availableTargetColumns.mkString(", ")}\n")

            // Get source columns
            _ <- Console[IO].println("📊 Analyzing source query...")
            sourceColumns <- getSourceColumns(config.sourceQuery, sourceConn)
            _ <- Console[IO].println(s"   Source columns (${sourceColumns.size}): ${sourceColumns.mkString(", ")}")

            // Determine target columns
            targetColumns = config.targetColumns.getOrElse(sourceColumns)
            _ <- if (config.targetColumns.isDefined)
              Console[IO].println(s"   Target columns (${targetColumns.size}): ${targetColumns.mkString(", ")}")
            else
              Console[IO].println(s"   Using source column names for target")

            // Validate column mapping
            _ <- validateColumnMapping(sourceColumns, targetColumns, availableTargetColumns)
            _ <- Console[IO].println(s"✓ Column mapping validated\n")

            totalRows <- countRows(config.sourceQuery, sourceConn)
            _ <- Console[IO].println(s"📈 Total rows to copy: $totalRows")
            _ <- Console[IO].println(
              s"⚙️  Batch size: ${config.batchSize}, Throttle: ${config.throttleDelay.toMillis}ms\n",
            )

            startTime <- IO.realTime
            _ <- Console[IO].println("🚀 Starting copy operation...\n")
            copied <- copyData(config, targetColumns, sourceConn, targetConn, targetConfig.schema, totalRows)

            endTime <- IO.realTime
            duration = (endTime - startTime).toSeconds
            _ <- Console[IO].println(s"\n✅ Copy completed successfully!")
            _ <- Console[IO].println(s"📦 Total rows copied: $copied")
            _ <- Console[IO].println(s"🎯 Target: $fullTargetName")
            _ <- Console[IO].println(s"⏱️  Duration: ${duration}s")
            _ <- if duration > 0 then
              Console[IO].println(f"⚡ Average speed: ${copied.toDouble / duration}%.2f rows/sec")
            else Console[IO].println(s"⚡ Average speed: $copied rows/sec")
          } yield copied
      }
    } yield result

  override def run: IO[Unit] = {
    val program = for {
      _ <- Console[IO].println("\n" + "=" * 70)
      _ <- Console[IO].println("🔄 PostgreSQL Data Copy Tool")
      _ <- Console[IO].println("=" * 70 + "\n")
      sourceConfig <- readDbConfig("SOURCE").handleErrorWith { e =>
        Console[IO].errorln(s"❌ Error reading SOURCE config: ${e.getMessage}") *> IO.raiseError(e)
      }
      targetConfig <- readDbConfig("TARGET").handleErrorWith { e =>
        Console[IO].errorln(s"❌ Error reading TARGET config: ${e.getMessage}") *> IO.raiseError(e)
      }
      copyConfig <- readCopyConfig.handleErrorWith { e =>
        Console[IO].errorln(s"❌ Error reading copy config: ${e.getMessage}") *> IO.raiseError(e)
      }
      _ <- Console[IO].println(s"🔌 Source: ${sourceConfig.connectionString}")
      _ <- Console[IO].println(s"🔌 Target: ${targetConfig.connectionString}")
      _ <- Console[IO].println("")
      _ <- performCopy(copyConfig, sourceConfig, targetConfig)
      _ <- Console[IO].println("\n" + "=" * 70 + "\n")
    } yield ()

    program.handleErrorWith { e =>
      Console[IO].errorln(s"\n❌ Fatal error: ${e.getMessage}") *>
        Console[IO].errorln(s"   Type: ${e.getClass.getSimpleName}\n") *>
        IO.raiseError(e)
    }
  }
}
