using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Data.OleDb;
using Sasa;
using Sasa.Linq;
using Sasa.Collections;

namespace DbCache
{
    static class Program
    {
        /// <summary>
        /// Message describing how to use the program.
        /// </summary>
        const string usage =
@"Usage: dbcache /in:<config> /out:<output>

  /in:  input configuration file
  /out: output C# source code file";

        /// <summary>
        /// Top-level configuration object.
        /// </summary>
        struct Config
        {
            /// <summary>
            /// DB connection string.
            /// </summary>
            public string DB { get; set; }
            /// <summary>
            /// Database connection type.
            /// </summary>
            public string DbType { get; set; }
            /// <summary>
            /// Included namespaces.
            /// </summary>
            public HashSet<string> Namespaces { get; set; }
            /// <summary>
            /// Table name to TableMapping map.
            /// </summary>
            public Dictionary<string, EnumMapping> Mappings { get; set; }
            /// <summary>
            /// The file to which the output is written.
            /// </summary>
            public string OutputFile { get; set; }
        }
        /// <summary>
        /// A table to enum mapping declaration.
        /// </summary>
        sealed class EnumMapping
        {
            public EnumMapping()
            {
                Columns = new Dictionary<string, EnumExtension>();
            }
            /// <summary>
            /// Table name.
            /// </summary>
            public string TableName { get; set; }
            /// <summary>
            /// Enum type name.
            /// </summary>
            public string EnumFQN { get; set; }
            /// <summary>
            /// Enum's field name.
            /// </summary>
            public string ColumnName { get; set; }
            /// <summary>
            /// Name of primary key.
            /// </summary>
            public string PK { get; set; }
            /// <summary>
            /// Mapped table columns.
            /// </summary>
            public Dictionary<string, EnumExtension> Columns { get; private set; }
            /// <summary>
            /// Cached table data.
            /// </summary>
            public TableData Table { get; set; }
            /// <summary>
            /// Attributes attached to the enum.
            /// </summary>
            public IEnumerable<string> Attributes { get; set; }

            public IEnumerable<KeyValuePair<string, object>> GetDefinition(out string ns, out string type)
            {
                var i = EnumFQN.LastIndexOf('.');
                type = i < 0 ? EnumFQN : EnumFQN.Remove(0, i + 1);
                ns = i < 0 ? "" : EnumFQN.Substring(0, i);
                return Table.Rows.Select(x => new KeyValuePair<string, object>(x.Cells[ENUM_ENTRY].ToString(), x.Cells[PK]));
            }

            /// <summary>
            /// Generate a reference to the enum's name for a given PK value.
            /// </summary>
            /// <param name="pkValue"></param>
            /// <returns></returns>
            public string NameFromPK(object pkValue)
            {
                if (pkValue.IsNull()) return null;
                foreach (var row in Table.Rows)
                {
                    if (row.Cells[PK].Equals(pkValue))
                        return row.Cells[ENUM_ENTRY].ToString();
                }
                if (!System.Diagnostics.Debugger.IsAttached)
                    Console.ReadLine();
                throw new ArgumentException("Table " + TableName + " has no PK=" + pkValue);
            }
        }
        /// <summary>
        /// A column to extension method mapping declaration.
        /// </summary>
        sealed class EnumExtension
        {
            ///// <summary>
            ///// Table this column belongs to.
            ///// </summary>
            //public TableMapping Table { get; set; }
            /// <summary>
            /// The function name this column will be translated to.
            /// </summary>
            public string Function { get; set; }
            /// <summary>
            /// Template expression used to compute the return value of the function.
            /// </summary>
            public string Expression { get; set; }
            /// <summary>
            /// Fully-qualified name of the return type.
            /// </summary>
            public string ReturnType { get; set; }
            /// <summary>
            /// True if the column is nullable.
            /// </summary>
            public bool IsNullable { get; set; }

            public bool IsFK { get; set; }
        }
        /// <summary>
        /// Encapsulates all relevant column and row data and metadata of a given table.
        /// </summary>
        sealed class TableData
        {
            public TableData(EnumMapping map)
            {
                Rows = new List<RowData>();
                Schema = new Dictionary<string, Type>();
                map.Table = this;
            }
            /// <summary>
            /// The column metadata, ie. name and type info.
            /// </summary>
            public Dictionary<string, Type> Schema { get; private set; }
            /// <summary>
            /// The rows in this table.
            /// </summary>
            public List<RowData> Rows { get; private set; }
        }
        /// <summary>
        /// Encapsulates all data and metadata for a row in a given table.
        /// </summary>
        sealed class RowData
        {
            public RowData()
            {
                Cells = new Dictionary<string, object>();
            }
            /// <summary>
            /// The values in the row.
            /// </summary>
            public Dictionary<string, object> Cells { get; private set; }
        }

        // Extract set of named tables into set of enums with appropriate extension methods
        // mapping primary keys to the respective rows.
        // 1. extract table mappings
        // 2. construct SELECT query for each mapping
        // 3. extract table data and metadata from data reader
        // 4. for each mapping, construct a lazy type graph
        // 5. compile the type graph to a set of concrete type and function definitions
        // 6. write the concrete types and functions to a C# file
        static void Main(string[] args)
        {
            try
            {
                var cfg = Parse(args);
                using (var conn = GetConn(cfg.DbType, cfg.DB))
                {
                    conn.Open();
                    foreach (var table in cfg.Mappings.Values)
                    {
                        Schema(table, conn);
                    }
                }
                Compile(cfg.Mappings);
                Output(cfg.Mappings, cfg.OutputFile, cfg.Namespaces);
                Console.WriteLine("Mapping successfully generated...");
            }
            catch (Exception ex)
            {
#if DEBUG
                Console.WriteLine(ex);
                Console.ReadLine();
#else
                Console.WriteLine(ex.Message);
#endif
            }
        }
        #region Output function
        /// <summary>
        /// Write out the generated program as a C# file.
        /// </summary>
        /// <param name="env"></param>
        /// <param name="file"></param>
        /// <param name="namespaces"></param>
        static void Output(Dictionary<string, EnumMapping> mappings, string file, HashSet<string> namespaces)
        {
            if (File.Exists(file)) File.Delete(file);
            using (var op = File.CreateText(file))
            {
                // included namespaces
                op.WriteLine("using System;");
                foreach (var ns in namespaces) op.WriteLine("using {0};", ns);
                op.WriteLine();

                // write out enum declaration with its stub
                foreach (var type in mappings.Values)
                {
                    // enum namespace
                    string ns, name;
                    var values = type.GetDefinition(out ns, out name);
                    if (!string.IsNullOrEmpty(ns))
                    {
                        op.WriteLine("namespace {0}", ns);
                        op.WriteLine('{');
                    }
                    // write out enum definition
                    foreach (var attr in type.Attributes)
                    {
                        op.WriteLine("    [{0}]", attr);
                    }
                    op.WriteLine("    public enum {0}", name);
                    op.WriteLine("    {");
                    foreach (var x in values)
                    {
                        op.WriteLine("        {0} = {1},", x.Key, x.Value);
                    }
                    op.WriteLine("    }");

                    // write out any column functions, if applicable
                    if (type.Columns.Count > 0)
                    {
                        op.WriteLine("    public static partial class {0}Extensions", name);
                        op.WriteLine("    {");
                        foreach (var fn in type.Columns)
                        {
                            // each function is an extension method which switches
                            // on the enum value and maps it to another value

                            op.WriteLine("        public static {0} {1}(this {2} value)",
                                fn.Value.ReturnType, string.IsNullOrEmpty(fn.Value.Function) ? fn.Key : fn.Value.Function, name);
                            op.WriteLine("        {");
                            if (string.Equals(fn.Key, type.PK, StringComparison.OrdinalIgnoreCase))
                            {
                                op.WriteLine("            return ({0})value;", fn.Value.ReturnType);
                            }
                            else
                            {
                                op.WriteLine("            switch (value)");
                                op.WriteLine("            {");
                                foreach (var row in type.Table.Rows)
                                {
                                    var expr = !string.IsNullOrEmpty(fn.Value.Expression)
                                             ? fn.Value.Expression.Replace('%' + fn.Key + '%',row.Cells[fn.Key].ToString())
                                             : Quote(row.Cells[fn.Key], fn.Value.IsFK, fn.Value.ReturnType);
                                    op.WriteLine("                case {0}.{1}: return {2};",
                                                 name, row.Cells[ENUM_ENTRY], expr);
                                }
                                op.WriteLine("                default: throw new ArgumentException(\"Invalid {0} provided.\");",
                                             type.EnumFQN);
                                op.WriteLine("            }");
                            }
                            op.WriteLine("        }");
                        }
                        op.WriteLine("    }");
                    }
                    if (!string.IsNullOrEmpty(ns))
                    {
                        op.WriteLine('}');
                    }
                }
            }
        }

        #endregion
        /// <summary>
        /// Quote the given value as a C# expression given the actual and expected type.
        /// </summary>
        /// <param name="val"></param>
        /// <param name="expected"></param>
        /// <returns></returns>
        static string Quote(object val, bool fk, string expectedType)
        {
            return val.IsNull()  ? "default(" + expectedType + ")":
                   val is string && !fk ? "\"" + (val as string).Replace("\"", "\\\"") + "\"":
                   val is bool   ? val.ToString().ToLower():
                   val is decimal? val.ToString() + "M":
                   val is float  ? val.ToString() + "F":
                                   val.ToString();
        }
        static string Normalize(char c, bool skip)
        {
            switch (c)
            {
                case '%': return "Percent";
                case '_':
                case '-': return "_";
                default:
                    return !char.IsLetterOrDigit(c) ? null:
                            skip && char.IsLower(c) ? char.ToUpper(c).ToString():
                                                      c.ToString();
            }
        }
        /// <summary>
        /// If <paramref name="name"/> starts with a number, this function
        /// parses that number and returns the index of the char after the
        /// number ends.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="sb"></param>
        /// <returns></returns>
        static int SkipNum(string name, StringBuilder sb)
        {
            int i = 0;
            while(i < name.Length && char.IsDigit(name[i])) ++i;
            if (i > 0)
            {
                var n = int.Parse(name.Substring(0, i));
                sb.Append(Normalize(Number.ToSentence(n)));
            }
            return i;
        }
        /// <summary>
        /// Normalize the given string as a valid C# identifier.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        static string Normalize(this string name)
        {
            var sb = new StringBuilder();
            var skip = true;
            for (int i = SkipNum(name, sb); i < name.Length; ++i)
            {
                var w = Normalize(name[i], skip);
                if (w == null) { skip = true; continue; }
                sb.Append(w);
                skip = false;
            }
            return sb.ToString();
        }
        /// <summary>
        /// Checks whether object is null or an instance of DBNull.
        /// </summary>
        /// <param name="o"></param>
        /// <returns></returns>
        static bool IsNull(this object o)
        {
            return o == null || o is DBNull;
        }
        #region Database Schema
        static void Schema(EnumMapping map, DbConnection conn)
        {
            var cmd = conn.CreateCommand();
                cmd.CommandText = string.Format(
                    "SELECT {0} FROM {1}",
                    map.PK.Push(map.ColumnName.Push(map.Columns.Keys)).Format(","), map.TableName);

            // load all table values into TableData and DataRow
            using (var reader = cmd.ExecuteReader(CommandBehavior.KeyInfo))
            {
                var data = new TableData(map);
                FillSchema(map, reader);
                FillData(data, map, reader);
            }
        }
        static Type nullable = typeof(Nullable<>);
        static void FillSchema(EnumMapping map, DbDataReader reader)
        {
            // load column definitions, ie. names and types, starting with
            // PK and name column
            map.Table.Schema[map.PK] = reader.GetFieldType(0);
            map.Table.Schema[ENUM_ENTRY] = reader.GetFieldType(1);
            var schema = reader.GetSchemaTable();
            //if (!System.Diagnostics.Debugger.IsAttached)
            //    Console.ReadLine();
            foreach (var col in map.Columns)
            {
                var i = reader.GetOrdinal(col.Key);
                var rcol = schema.Rows[i];
                col.Value.IsNullable = (bool)rcol["AllowDBNull"];
                map.Table.Schema[col.Key] = reader.GetFieldType(i);
            }
        }
        const string ENUM_ENTRY = "__enum_entry";
        static void FillData(TableData table, EnumMapping map, DbDataReader reader)
        {
            // load row data, including PK and name columns
            foreach (IDataRecord drow in reader)
            {
                // copy the raw data from the reader to TableData
                var row = new RowData
                {
                    Cells =
                    {
                        { map.PK, drow.GetValue(0) },
                        { ENUM_ENTRY, Normalize(drow.GetValue(1).ToString()) },
                    }
                };
                table.Rows.Add(row);
                foreach (var col in map.Columns)
                {
                    row.Cells[col.Key] = drow[col.Key];
                }
            }
        }
        #endregion
        /// <summary>
        /// Compile the given table with the given environment and table mappings.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="mappings"></param>
        /// <param name="env"></param>
        static void Compile(Dictionary<string, EnumMapping> mappings)
        {
            // iterate through all mappings and for every mapped column that:
            // 1. has an explicit return type, map the row[column].value to the corresponding enum name,
            // 2. is nullable, then change the return type
            foreach (var map in mappings.Values)
            {
                foreach (var col in map.Columns)
                {
                    // update the return type mapping of each column
                    EnumMapping fk;
                    if (!string.IsNullOrEmpty(col.Value.ReturnType) && string.IsNullOrEmpty(col.Value.Expression) && mappings.TryGetValue(col.Value.ReturnType, out fk))
                    {
                        col.Value.IsFK = true;
                        foreach (var row in map.Table.Rows)
                        {
                            var x = fk.NameFromPK(row.Cells[col.Key]);
                            row.Cells[col.Key] = x == null ? null : fk.EnumFQN + '.' + x;
                        }
                    }
                    // if column is a nullable struct, then wrap in System.Nullable
                    var colType = map.Table.Schema[col.Key];
                    var baseType = string.IsNullOrEmpty(col.Value.ReturnType) ? colType.FullName : col.Value.ReturnType;
                    var type = col.Value.IsNullable && colType.IsValueType
                             ? "System.Nullable<" + baseType + ">"
                             : baseType;
                    col.Value.ReturnType = type;
                }
            }
        }
        #region Parsing functions
        /// <summary>
        /// Convenient short hand for splitting strings.
        /// </summary>
        /// <param name="token"></param>
        /// <param name="input"></param>
        /// <returns></returns>
        static string[] Split(string token, string input)
        {
            return input.Split(StringSplitOptions.RemoveEmptyEntries, token);
        }
        /// <summary>
        /// Database connection factory.
        /// </summary>
        /// <param name="connType"></param>
        /// <param name="db"></param>
        /// <returns></returns>
        static DbConnection GetConn(string connType, string db)
        {
            switch (connType)
            {
                case "SqlClient": return new SqlConnection(db);
                default:          return new OleDbConnection(db);
            }
        }
        /// <summary>
        /// Search the given config values by key, and return the matches, if any.
        /// </summary>
        /// <param name="args"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        static IEnumerable<string> FindByKey(this string[] args, string key)
        {
            return from a in args
                   where a.StartsWith(key, StringComparison.InvariantCultureIgnoreCase)
                      && !char.IsLetterOrDigit(a[key.Length])
                   select a.Substring(a.IndexOf('=') + 1).Trim();
        }
        /// <summary>
        /// Search the given config values by key, and return otherwise if key not found.
        /// </summary>
        /// <param name="args"></param>
        /// <param name="key"></param>
        /// <param name="otherwise"></param>
        /// <returns></returns>
        static string FindByKey(this string[] args, string key, string otherwise)
        {
            var result = args.FindByKey(key).FirstOrDefault() ?? otherwise;
            Error(result == null, "Missing '" + key + "'.", null);
            return result;
        }
        /// <summary>
        /// Throw a parse error with line number info.
        /// </summary>
        /// <param name="cond"></param>
        /// <param name="err"></param>
        /// <param name="line"></param>
        static void Error(bool cond, string err, int? line)
        {
            if (cond) throw new ArgumentException(
                line == null ? err:
                               string.Format("ERROR: line {0}, {1}.", line.Value+1, err));
        }
        /// <summary>
        /// Parse the mapping files and command-line arguments.
        /// </summary>
        /// <param name="config"></param>
        /// <param name="args"></param>
        /// <returns></returns>
        static Config Parse(string[] args)
        {
            string[] config = null;
            var map = new Dictionary<string, EnumMapping>();
            var cfg = new Config { Namespaces = new HashSet<string>(), Mappings = map };
            var included = new HashSet<string>();
            string pwd = string.Empty;
            // parse command-line arguments
            foreach (var a in args)
            {
                if (a.StartsWith("/out:")) cfg.OutputFile = a.Substring("/out:".Length);
                else if (a.StartsWith("/in:"))
                {
                    var file = Path.GetFullPath(a.Substring("/in:".Length));
                    included.Add(file);
                    config = File.ReadAllLines(file);
                    pwd = Path.GetDirectoryName(file);
                }
                else if (a.StartsWith("/?"))
                {
                    throw new ArgumentException(usage);
                }
            }
            // ensure we have input and output files
            Error(config == null || cfg.OutputFile == null, usage, null);
            return Parse(cfg, pwd, config, included);
        }
        /// <summary>
        /// Process a config file.
        /// </summary>
        /// <param name="cfg">The configuration to use.</param>
        /// <param name="pwd">Present working directory, from which all file ::include directives are resolved.</param>
        /// <param name="config">The lines of the current config file being processed.</param>
        /// <param name="included">The set of currently visited config files.</param>
        /// <returns>An initialized configuration.</returns>
        static Config Parse(Config cfg, string pwd, string[] config, HashSet<string> included)
        {
            var map = cfg.Mappings;
            // parse mapping file
            for (var i = 0; i < config.Length; ++i)
            {
                var cmd = config[i];
                if (cmd.StartsWith("::database"))
                {
                    // declare the database type and connection string
                    var line = config[i].Split('=');
                    Error(line.Length < 2, "Improper ::database declaration", i);
                    var dbType = line[1].Trim();
                    cfg.DbType = dbType;
                    Error(++i >= config.Length, "Missing ::database connection string", i);
                    cfg.DB = config[i].Trim();
                }
                else if (cmd.StartsWith("::using"))
                {
                    // included namespaces in output file
                    var line = config[i].Split('=');
                    Error(line.Length < 2, "Improper ::using declaration", i);
                    var ns = line[1].Trim();
                    cfg.Namespaces.Add(ns);
                }
                else if (cmd.StartsWith("::include "))
                {
                    var path = Path.Combine(pwd, cmd.Substring("::include ".Length));
                    Error(string.IsNullOrEmpty(path), "Please provide a valid ::include path.", i);
                    Error(!File.Exists(path), "File specified by ::include does not exist.", i);
                    if (included.Contains(path)) continue;
                    cfg = Parse(cfg, pwd, File.ReadAllLines(path), included);
                }
                else if (config[i].StartsWith("::table"))
                {
                    // extract top-level table info, ie. pk, enum name, etc.
                    var table = Split("::", config[i]);
                    Error(table.Length < 4, "::table requires table, enum, pk, and name specified.", i);
                    var tname = table.FindByKey("table", null);
                    var tableMap = new EnumMapping
                    {
                        TableName = tname,
                        EnumFQN = table.FindByKey("enum", tname),
                        PK = table.FindByKey("pk", null),
                        ColumnName = table.FindByKey("name", null),
                        Attributes = table.FindByKey("attr"),
                    };
                    // extract column info from mapping file
                    for (++i; i < config.Length && !config[i].StartsWith("::"); ++i)
                    {
                        Error(string.IsNullOrEmpty(config[i]), "Missing column information for table " + tname, i);
                        var column = Split("::", config[i]);
                        var name = column[0].Trim();
                        tableMap.Columns[name] = new EnumExtension
                        {
                            //Table = tableMap,
                            Function = column.FindByKey("function", name),
                            Expression = column.FindByKey("expression", ""),
                            ReturnType = column.FindByKey("returnType", ""),
                        };
                    }
                    // undo the last increment after processing columns, since
                    // outer loop increments i again
                    --i;
                    map[tableMap.EnumFQN] = tableMap;
                }
            }
            return cfg;
        }
        #endregion
    }
}
