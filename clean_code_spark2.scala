
import scala.util.{Try, Success, Failure}
import java.net.URI
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

//########################## definitions: ##########################
// this code has been run on Spark 2.3, but should be ok on any version of Spark 2+

val BASEDIR = "file:///home/kp807/projects/scifen/"
val NO_URL = "NO.URL"
val sldfilename = s"$BASEDIR/SLDs.csv"
val arcjsonschemafilename = s"$BASEDIR/arcwat_schema.json" 
val warcjsonschemafilename = s"$BASEDIR/warcwat_schema.json" 
val ARC_DFSCHEMA = spark.read.option("multiline", true).json(arcjsonschemafilename).schema
val WARC_DFSCHEMA = spark.read.option("multiline", true).json(warcjsonschemafilename).schema
val SLD_LIST = spark.read.option("header", "false").csv(sldfilename).drop("_c0").as[String].map(r => r.drop(1)).collect.toSet

val HDFS_PREFIX_DIR = "/user/kp807/earlyweb/"

// these are the fields to extract from the arc and warc files - they are slightly different: 
val arccols = "Envelope.Payload-Metadata.HTTP-Response-Metadata.HTML-Metadata Envelope.ARC-Header-Metadata.Target-URI Envelope.ARC-Header-Metadata.Date Envelope.ARC-Header-Metadata.Content-Type Envelope.ARC-Header-Metadata.Content-Length".split(" ")
val warccols = "Envelope.Payload-Metadata.HTTP-Response-Metadata.HTML-Metadata Envelope.WARC-Header-Metadata.WARC-Target-URI Envelope.WARC-Header-Metadata.WARC-Date Envelope.WARC-Header-Metadata.Content-Type Envelope.WARC-Header-Metadata.Content-Length".split(" ")

// create expressions that extract given column names in the standard format
// ARC and WARC have different names of fields
// this function will be applied to arccols and warccols
def getExtractionColums(cols:Array[String]) = {
  val cols2 = cols.map(cc => s"${cc.split("\\.").map(pp => "`"+pp+"`").mkString(".")}") //Spark doesn't like '-' in the name of a column, so back-tick every part of the column name
  val colsnew = "links target date contenttype contentlength".split(" ")                // new column names
  val colExpr = cols2.zip(colsnew).map({case (ccorig, ccnew) => s"$ccorig AS $ccnew"})
  colExpr
}

val arcColExpr = getExtractionColums(arccols)
val warcColExpr = getExtractionColums(warccols)

//###############3 these are utility functions that deal with moving data around and processing 13 parts of earlyweb
val years = "1996 1997 1998 1999 2000".split(" ").map(_.toInt)
val parts = "1 1 1 3 10".split(" ").map(_.toInt)
val years_parts = years.zip(parts.map(i => (1 to i).toArray)).map({case (x, mylist) => mylist.map(i => (x,i))}).flatMap(x => x)

def getOriginalInputPath(year:Int, parti:Int) = s"hdfs:///nsfia/input/earlyweb/$year/part$parti"
def getExtractedFilenamePath(year:Int, parti:Int) = s"hdfs:///user/kp807/earlyweb/${year}_part${parti}_links.parquet"
def getLocalExtractedFilenamePath(year:Int, parti:Int) = s"file:///lustre/cs1500/kp807/earlyweb/earlyweb/${year}_part${parti}_links.parquet"
def getLocalReportName(year:Int, parti:Int) = s"file:///home/kp807/projects/scifen/results/${year}_part${parti}_report.parquet"                  // Apr 17, 2018: moved these files to f_msw118_1
def getLocalWebergroupReportName(year:Int, parti:Int) = s"file:///home/projects/f_msw118_1/reports_parquet/${year}_part${parti}_report.parquet"  // new on Apr 17, 2018
def getLocalWebergroupRawName(year:Int, parti:Int) = s"file:///home/projects/f_msw118_1/earlyweb_raw_links/${year}_part${parti}_links.csv"  // new on Apr 17, 2018

val infiles = years_parts.map({case(year, parti) => getOriginalInputPath(year, parti) })
val outfiles = years_parts.map({case(year, parti) => getLocalReportName(year, parti) })

//########################### functions ##########################

// utility function for reading local file
def getLines(filename:String) = scala.io.Source.fromFile(filename).getLines().toArray

// processing the intermediate dataframe that extracted data from json: 
def extractDataFromDataFrame(df:DataFrame) = {
       // add shortdate field from date
       val flatdf = df.selectExpr("*", "SUBSTRING(date, 0, 8) AS shortdate")
       // Links is an array, but we want each link as a separate record, there use "explode" function
       val flatdf2 = flatdf.select( explode($"links.Links").as("urls"), $"target", $"contenttype", $"contentlength", $"shortdate" )
       // renaming fields
       val flatdf3 = flatdf2.selectExpr("target", "urls.url AS url", "urls.text AS text", "shortdate", "contenttype", "contentlength")
       flatdf3
}

// getting the actual output for raw links: 
def processLinks(inputfilename:String, outputfilename:String, filterHttpLike:Boolean = true, arc_or_warc:String = "arc") = {
    val (colExpr, dfreader) = arc_or_warc match { 
          case "arc" => (arcColExpr, spark.read.schema(ARC_DFSCHEMA) )
          case "warc" => (warcColExpr, spark.read.schema(WARC_DFSCHEMA) ) 
          case _  => {println(s"used unknown option $arc_or_warc : please use 'arc' or 'warc'. Defaulting to 'arc'"); (arcColExpr, spark.read.schema(ARC_DFSCHEMA) )}
    }
    val df = dfreader.json(inputfilename).selectExpr(colExpr: _*).filter("links IS NOT NULL")
    val resultdf = extractDataFromDataFrame(df)
    val resultdf2 = if (filterHttpLike) resultdf.filter("url like 'http%'") else resultdf
    resultdf.write.parquet(outputfilename)
}

def processLinksFiles(inout:Array[(String, String)]) = {
    inout.foreach({case (inputfilename, outputfilename) => {
       println(s"doing $inputfilename, $outputfilename")
       Try(processLinks(inputfilename, outputfilename))
    }})
}

//###################### functions that produce second level domain reports ####################

def isUrl(url:String) = """http[s]?://.*\..*""".r.findFirstIn(url).nonEmpty

def getDomainName(url:String) =  {
    val uri = new URI(url);
    val domain = uri.getHost();
    if (domain.startsWith("www.")) domain.substring(4) else domain; //drop www.  if it is part of URI
}

def getExactLevelDomain(url:String) = {
    val domainName = Try(getDomainName(url)) match {
       case Success(dn) => dn
       case Failure(_)   => NO_URL
    }
    val sld = domainName.split("\\.").takeRight(2).mkString(".")  // only 2 last domains  e.g. help.rutgers.edu --> rutgers.edu
    val tld = domainName.split("\\.").takeRight(3).mkString(".")  // only 3 last domains  e.g. oit.help.rutgers.edu --> help.rutgers.edu
    if (SLD_LIST.contains(sld)) tld else sld                      // SLD_LIST is a list of non-informative domains such as co.uk 
}

val sqlContext = spark.sqlContext
sqlContext.udf.register("getExactLevelDomain", getExactLevelDomain: String => String)


// infile = local directory for the parquet file that was the extracted links
// outfile = name of the output file
def getReport(infile:String, outfile:String) = {
    val df = spark.read.parquet(infile)
    val df2 = df.filter("url like 'http%'").
                 selectExpr("*",
                            "getExactLevelDomain(target) AS src",
                            "getExactLevelDomain(url) AS dst",
                            "CAST (contentlength AS bigint) AS contentlength2")
    val res = df2.groupBy("src", "dst", "shortdate").agg(count(lit(1)).as("record_count"), sum("contentlength2").as("content_length"))
    //res.write.parquet(outfile)
    res.sort("shortdate", "src", "dst").write.parquet(outfile)
    //res.sort("shortdate", "src", "dst").write.csv(outfile)
}


//def convertParquetToCsv(infile:String) = { spark.read.parquet(infile).sort("shortdate", "src", "dst").coalesce(10).write.csv(infile.split("\\.").head+".csv")}
def convertParquetToCsv(infile:String, outfile:String) = { spark.read.parquet(infile).write.csv(outfile)}


/* running the functions/script:
start spark-shell  and paste the appropriate part of the code

:load /home/kp807/projects/scifen/clean_code_spark2.scala
getReport(getLocalExtractedFilenamePath(2000, 1), getLocalReportName(2000, 1))

years_parts.drop(10).foreach({case (year, parti) =>{ 
   val infile = getLocalExtractedFilenamePath(year, parti)
   val outfile = getLocalWebergroupRawName(year, parti); 
   println(s"converting $infile  to $outfile")
   convertParquetToCsv(infile, outfile) } })

//senate data
processLinks("/home/projects/f_msw118_1/nsfia/input/senate/arc/", "/home/projects/f_msw118_1/senate_raw_links/senate_links_from_arc.parquet", filterHttpLike = false)
convertParquetToCsv("/home/projects/f_msw118_1/senate_raw_links/senate_links_from_arc.parquet", "/home/projects/f_msw118_1/senate_raw_links/senate_links_from_arc.csv")
processLinks("/home/projects/f_msw118_1/nsfia/input/senate/warc/", "/home/projects/f_msw118_1/senate_raw_links/senate_links_from_warc.parquet", false, "warc") 
convertParquetToCsv("/home/projects/f_msw118_1/senate_raw_links/senate_links_from_warc.parquet", "/home/projects/f_msw118_1/senate_raw_links/senate_links_from_warc.csv")

processLinks("/home/projects/f_msw118_1/nsfia/input/house/arc/", "/home/projects/f_msw118_1/house_raw_links/house_links_from_arc.parquet", filterHttpLike = false)
convertParquetToCsv("/home/projects/f_msw118_1/house_raw_links/house_links_from_arc.parquet", "/home/projects/f_msw118_1/house_raw_links/house_links_from_arc.csv")  

processLinks("/home/projects/f_msw118_1/nsfia/input/house/warc/", "/home/projects/f_msw118_1/house_raw_links/house_links_from_warc.parquet", false, "warc") 
convertParquetToCsv("/home/projects/f_msw118_1/house_raw_links/house_links_from_warc.parquet", "/home/projects/f_msw118_1/house_raw_links/house_links_from_warc.csv")

spark.read.parquet("/home/projects/f_msw118_1/house_raw_links/house_links_from_arc.parquet").repartition(100).write.csv("/home/projects/f_msw118_1/house_raw_links/house_links_from_arc.parquet.100files")

*/
