simplifying-jdbc
================

A particularly nice example is a wrapper for simplifying JDBC, written by Ross Judson, showing Pimp my Library pattern as described by Martin Odersky


Scala’s syntax can make typing-intensive tasks a lot easier to handle.
Here’s one way to “Scalify” a Java library. I make liberal use of
operators and implicit conversions to achieve a terse and readable
syntax for handling JDBC interactions.

It’s very convenient in Scala to do “back-and-forth” implicit
conversions. We declare a “Rich” version of an object and place
additional methods/syntax we want on the “Rich” version. Two implicit
conversions are then defined; a “type2Rich” and a “rich2Type”. These
seamlessly invoke our extended functionality.

First we’ll show example usage, followed by the RichSQL object
definition that creates our syntactic environment.

``` {.code .scala}
package f2;
 
// In Scala we can specify multiple imports from the same package
// by combining them together. 
import java.sql.{DriverManager, Connection, ResultSet, PreparedStatement, Statement, Date };
// We can also rename an import to avoid a name collision. Here
// we're changing the name of java.util.Random to JRandom.
import java.util.{Random => JRandom};
import DriverManager.{getConnection => connect};
 
// The underscore is used to import everything in a package or object.
// "Console" is a Scala singleton object; we are bringing its public
// declarations into our namespace. Note that when we do this we are
// bringing in types, methods, classes -- everything. 
import Console._;
 
/** TestF2 demonstrates the use of the RichSQL object's facilities
to make JDBC work easier than Java. */
object TestF2 {
 
    // Bring in the RichSQL public declarations. Note that when
    // we import an implicit declaration, that implicit becomes
    // part of our namespace.
    import RichSQL._;
 
    // a random number generator. Note that we're using our altered name.
    private val rnd = new JRandom();
    
    // commands to set up a simple database. Scala has multi-line
    // string constants, which are very handy when we want to embed
    // things like extended text, or sql commands.
    private val setup = Array(
"""
    drop table if exists person
""","""
    create table person(
        id identity,
        type int,
        name varchar not null)
""");
 
    // some additional data. Scala's type inferencing makes this into an 
    // Array[String] -- an Array of Strings.
    private val names = Array("Ross", "Lynn", "John", "Terri", "Steve", "Zobeyda");
    
    def go = {
        // Create a connection to the database. Catching exceptions is
        // optional in Scala...we can put this in a try block or ignore
        // the exceptions and expect something "higher up" to catch them.
        // We imported DriverManager's connect method earlier, so we're
        // calling that here.
        // "conn" is also typed as implicit, which means it will
        // be automatically used in any function call that 
        // requires a Connection in an implicit parameter position. 
        implicit val conn = connect("jdbc:h2:f2", "sa", "");
        
        // Our RichSQL environment does quite a bit for us
        // with minimal syntax. "setup" is an array of commands
        // to execute. Here we are triggering an implicit
        // conversion of "conn" to a RichConnection, which
        // is then passed an array of commands with the <<
        // operator. On RichConnection the << operator creates
        // a RichStatement object and passes the commands 
        // to it for execution. Finally, our variable "s"
        // is typed as a statement, so an implicit conversion
        // from RichStatement to Statement is performed. 
        // "s" is also typed as implicit, which means it will
        // be automatically used in any function call that 
        // requires a Statement in an implicit parameter position. 
        implicit val s: Statement = conn << setup;
 
        // Creates a JDBC prepared statement. We can omit the 
        // period and parentheses in simple calls; x.f(y) can
        // be written as x f y.
        val insertPerson = conn prepareStatement "insert into person(type, name) values(?, ?)";
        // For each name in our list of names, load the prepared
        // statement with a random number and the name, then
        // execute it. Our RichPreparedStatement has type-specific
        // overloadings for the << operator which call the right
        // setXXX methods on the JDBC PreparedStatement. I've removed
        // the spaces between operators and identifiers here to 
        // show that there are natural boundaries between operator characters
        // and identifiers. The <<! postfix operator calls the
        // PreparedStatement's execute method.
        for (val name <- names) 
            insertPerson<<rnd.nextInt(10)<<name<<!;
        
        // Execute a query against an implicit Statement. The 
        // query function looks for a Statement declared as "implicit"
        // in this namespace and uses it automatically. We also
        // supply a construction function that builds a Person object
        // from a RichResultSet. There are a variety of implicit 
        // conversions from RichResultSet to fundamental types. Each
        // invocation advances to the next column in the result set,
        // so Person(rs,rs,rs) makes three conversion calls. Since
        // Scala knows the types of the fields on Person, it knows
        // which implicit conversion functions to call. 
        // "query" is producing a lazy sequence of results; note that
        // the syntax for iterating over it is exactly the same as
        // those used earlier.
        // Once we've recovered our person object, we convert it to
        // XML and print it out.
        for (val person <- query("select * from person", rs => Person(rs,rs,rs)))
            println(person.toXML);
        
        // The same thing can be done with a RichPreparedStatement
        for (val person <- "select * from person" <<! (rs => Person(rs,rs,rs)))
            println(person.toXML);
     }
 
    /** A simple class holding information about a person. */
    case class Person(id: Long, tpe: Int, name: String) {
        def toXML = <person id={id.toString} type={tpe.toString}>{name}</person>
    }
    
    def main(args: Array[String]): Unit = {
        val h2driver = Class.forName("org.h2.Driver");
        go;
    }
    
    def p[X](x: X) = { println(x); x }
    
}
 
package f2;
 
import java.sql.{DriverManager, Connection, ResultSet, PreparedStatement, Statement, Date };
 
object RichSQL {
    private def strm[X](f: RichResultSet => X, rs: ResultSet): Stream[X] = 
        if (rs.next) Stream.cons(f(new RichResultSet(rs)), strm(f, rs))
        else { rs.close(); Stream.empty };
 
    implicit def query[X](s: String, f: RichResultSet => X)(implicit stat: Statement) = {
        strm(f,stat.executeQuery(s));
    }
    
    implicit def conn2Statement(conn: Connection): Statement = conn.createStatement;
    
    implicit def rrs2Boolean(rs: RichResultSet) = rs.nextBoolean;
    implicit def rrs2Byte(rs: RichResultSet) = rs.nextByte;
    implicit def rrs2Int(rs: RichResultSet) = rs.nextInt;
    implicit def rrs2Long(rs: RichResultSet) = rs.nextLong;
    implicit def rrs2Float(rs: RichResultSet) = rs.nextFloat;
    implicit def rrs2Double(rs: RichResultSet) = rs.nextDouble;
    implicit def rrs2String(rs: RichResultSet) = rs.nextString;
    implicit def rrs2Date(rs: RichResultSet) = rs.nextDate;
 
    implicit def resultSet2Rich(rs: ResultSet) = new RichResultSet(rs);
    implicit def rich2ResultSet(r: RichResultSet) = r.rs;
    class RichResultSet(val rs: ResultSet) {
        
      var pos = 1
      def apply(i: Int) = { pos = i; this }
        
      def nextBoolean: Boolean = { val ret = rs.getBoolean(pos); pos = pos + 1; ret }
      def nextByte: Byte = { val ret = rs.getByte(pos); pos = pos + 1; ret }
      def nextInt: Int = { val ret = rs.getInt(pos); pos = pos + 1; ret }
      def nextLong: Long = { val ret = rs.getLong(pos); pos = pos + 1; ret }
      def nextFloat: Float = { val ret = rs.getFloat(pos); pos = pos + 1; ret }
      def nextDouble: Double = { val ret = rs.getDouble(pos); pos = pos + 1; ret }
      def nextString: String = { val ret = rs.getString(pos); pos = pos + 1; ret }
      def nextDate: Date = { val ret = rs.getDate(pos); pos = pos + 1; ret }
 
      def foldLeft[X](init: X)(f: (ResultSet, X) => X): X = rs.next match {
        case false => init
        case true => foldLeft(f(rs, init))(f)
      }
      def map[X](f: ResultSet => X) = {
        var ret = List[X]()
        while (rs.next())
        ret = f(rs) :: ret
        ret.reverse; // ret should be in the same order as the ResultSet
      }
    }
 
    implicit def str2RichPrepared(s: String)(implicit conn: Connection): RichPreparedStatement = conn prepareStatement(s);
    
    implicit def ps2Rich(ps: PreparedStatement) = new RichPreparedStatement(ps);
    implicit def rich2PS(r: RichPreparedStatement) = r.ps;
 
    class RichPreparedStatement(val ps: PreparedStatement) {
      var pos = 1;
      private def inc = { pos = pos + 1; this }
 
      def execute[X](f: RichResultSet => X): Stream[X] = {
    pos = 1; strm(f, ps.executeQuery)
      }
      def <<![X](f: RichResultSet => X): Stream[X] = execute(f);
 
      def execute = { pos = 1; ps.execute }
      def <<! = execute;
 
      def <<(b: Boolean) = { ps.setBoolean(pos, b); inc }
      def <<(x: Byte) = { ps.setByte(pos, x); inc }
      def <<(i: Int) = { ps.setInt(pos, i); inc }
      def <<(x: Long) = { ps.setLong(pos, x); inc }
      def <<(f: Float) = { ps.setFloat(pos, f); inc }
      def <<(d: Double) = { ps.setDouble(pos, d); inc }      
      def <<(o: String) = { ps.setString(pos, o); inc }
      def <<(x: Date) = { ps.setDate(pos, x); inc }
    }
    
    implicit def conn2Rich(conn: Connection) = new RichConnection(conn);
    
    class RichConnection(val conn: Connection) {
      def <<(sql: String) = new RichStatement(conn.createStatement) << sql;
      def <<(sql: Seq[String]) = new RichStatement(conn.createStatement) << sql;
    }
 
    implicit def st2Rich(s: Statement) = new RichStatement(s);
    implicit def rich2St(rs: RichStatement) = rs.s;
    
    class RichStatement(val s: Statement) {
      def <<(sql: String) = { s.execute(sql); this }
      def <<(sql: Seq[String]) = { for (val x <- sql) s.execute(x); this }
    }
}
 
```

Here is more advanced version, capable to handling NULL values via
Option[T]

``` {.code .scala}
/*
 * RichSQL.scala
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */
 
 
import java.sql.ResultSet
import java.sql.PreparedStatement
import java.sql.Date
import java.sql.Statement
import java.sql.Connection
import java.sql.Types
import scala.runtime.RichBoolean
 
object RichSQL {
    private def strm[X](f: RichResultSet => X, rs: ResultSet): Stream[X] =
    if (rs.next) Stream.cons(f(new RichResultSet(rs)), strm(f, rs))
    else { rs.close(); Stream.empty };
 
    implicit def query[X](s: String, f: RichResultSet => X)(implicit stat: Statement) = {
        strm(f,stat.executeQuery(s));
    }
 
    implicit def conn2Statement(conn: Connection): Statement = conn.createStatement;
 
    implicit def rrs2Boolean(rs: RichResultSet) = rs.nextBoolean;
    implicit def rrs2Byte(rs: RichResultSet) = rs.nextByte;
    implicit def rrs2Int(rs: RichResultSet) = rs.nextInt;
    implicit def rrs2Long(rs: RichResultSet) = rs.nextLong;
    implicit def rrs2Float(rs: RichResultSet) = rs.nextFloat;
    implicit def rrs2Double(rs: RichResultSet) = rs.nextDouble;
    implicit def rrs2String(rs: RichResultSet) = rs.nextString;
    implicit def rrs2Date(rs: RichResultSet) = rs.nextDate;
 
    implicit def resultSet2Rich(rs: ResultSet) = new RichResultSet(rs);
    implicit def rich2ResultSet(r: RichResultSet) = r.rs;
 
    class RichResultSet(val rs: ResultSet) {
 
        var pos = 1
        def apply(i: Int) = { pos = i; this }
 
        def nextBoolean: Option[Boolean] = {val ret = rs.getBoolean(pos); pos = pos + 1; if(rs.wasNull) None else Some(ret) }
        def nextByte: Option[Byte] = { val ret = rs.getByte(pos); pos = pos + 1;  if(rs.wasNull) None else Some(ret) }
        def nextInt: Option[Int] = { val ret = rs.getInt(pos); pos = pos + 1; if(rs.wasNull) None else Some(ret) }
        def nextLong: Option[Long] = { val ret = rs.getLong(pos); pos = pos + 1; if(rs.wasNull) None else Some(ret) }
        def nextFloat: Option[Float] = { val ret = rs.getFloat(pos); pos = pos + 1;  if(rs.wasNull) None else Some(ret) }
        def nextDouble: Option[Double] = { val ret = rs.getDouble(pos); pos = pos + 1;  if(rs.wasNull) None else Some(ret) }
        def nextString: Option[String] = { val ret = rs.getString(pos); pos = pos + 1;  if(rs.wasNull) None else Some(ret) }
        def nextDate: Option[Date] = { val ret = rs.getDate(pos); pos = pos + 1;  if(rs.wasNull) None else Some(ret) }
 
        def foldLeft[X](init: X)(f: (ResultSet, X) => X): X = rs.next match {
            case false => init
            case true => foldLeft(f(rs, init))(f)
        }
        def map[X](f: ResultSet => X) = {
            var ret = List[X]()
            while (rs.next())
            ret = f(rs) :: ret
            ret.reverse; // ret should be in the same order as the ResultSet
        }
    }
 
 
    implicit def ps2Rich(ps: PreparedStatement) = new RichPreparedStatement(ps);
    implicit def rich2PS(r: RichPreparedStatement) = r.ps;
 
    implicit def str2RichPrepared(s: String)(implicit conn: Connection): RichPreparedStatement = conn prepareStatement(s);
    implicit def conn2Rich(conn: Connection) = new RichConnection(conn);
 
    implicit def st2Rich(s: Statement) = new RichStatement(s);
    implicit def rich2St(rs: RichStatement) = rs.s;
 
    class RichPreparedStatement(val ps: PreparedStatement) {
        var pos = 1;
        private def inc = { pos = pos + 1; this }
 
        def execute[X](f: RichResultSet => X): Stream[X] = {
            pos = 1; strm(f, ps.executeQuery)
        }
        def <<![X](f: RichResultSet => X): Stream[X] = execute(f);
 
        def execute = { pos = 1; ps.execute }
        def <<! = execute;
 
        def <<(x: Option[Any]):RichPreparedStatement = {
            x match {
                case None =>
                    ps.setNull(pos,Types.NULL)
                    inc
                case Some(y) => (this << y)
            }
        }
        def <<(x:Any):RichPreparedStatement ={
            x match {
                case z:Boolean =>
                    ps.setBoolean(pos, z)
                case z:Byte =>
                    ps.setByte(pos, z)
                case z:Int =>
                    ps.setInt(pos, z)
                case z:Long =>
                    ps.setLong(pos, z)
                case z:Float =>
                    ps.setFloat(pos, z)
                case z:Double =>
                    ps.setDouble(pos, z)
                case z:String =>
                    ps.setString(pos, z)
                case z:Date =>
                    ps.setDate(pos, z)
                case z => ps.setObject(pos,z)
            }
            inc
        }
    }
 
 
    class RichConnection(val conn: Connection) {
        def <<(sql: String) = new RichStatement(conn.createStatement) << sql;
        def <<(sql: Seq[String]) = new RichStatement(conn.createStatement) << sql;
    }
 
 
    class RichStatement(val s: Statement) {
        def <<(sql: String) = { s.execute(sql); this }
        def <<(sql: Seq[String]) = { for (val x <- sql) s.execute(x); this }
    }
}
```
