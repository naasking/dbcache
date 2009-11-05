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
using Sasa.String;

namespace DbCache
{
    static class Program
    {
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
            public List<string> Namespaces { get; set; }
            /// <summary>
            /// Table name to TableMapping map.
            /// </summary>
            public Dictionary<string, TableMapping> Mappings { get; set; }
            /// <summary>
            /// The file to which the output is written.
            /// </summary>
            public string OutputFile { get; set; }
        }
        /// <summary>
        /// A table to enum mapping declaration.
        /// </summary>
        sealed class TableMapping
        {
            /// <summary>
            /// Table name.
            /// </summary>
            public string Table { get; set; }
            /// <summary>
            /// Enum type name.
            /// </summary>
            public string EnumFQN { get; set; }
            /// <summary>
            /// Enum field name.
            /// </summary>
            public string Name { get; set; }
            /// <summary>
            /// Name of primary key.
            /// </summary>
            public string PK { get; set; }
            /// <summary>
            /// Mapped table columns.
            /// </summary>
            public Dictionary<string, ColumnMapping> Columns { get; set; }
            /// <summary>
            /// Cached table data.
            /// </summary>
            public TableData Data { get; set; }
        }
        /// <summary>
        /// A column to extension method mapping declaration.
        /// </summary>
        sealed class ColumnMapping
        {
            /// <summary>
            /// Table this column belongs to.
            /// </summary>
            public TableMapping Table { get; set; }
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
        }
        /// <summary>
        /// Encapsulates all relevant column and row data and metadata of a given table.
        /// </summary>
        sealed class TableData
        {
            public TableData()
            {
                Rows = new List<RowData>();
                Columns = new Dictionary<string, Type>();
            }
            /// <summary>
            /// The column metadata, ie. name and type info.
            /// </summary>
            public Dictionary<string, Type> Columns { get; private set; }
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
        /// <summary>
        /// Top-level environment. This holds the final result of the compilation,
        /// ie. all type and function definitions.
        /// </summary>
        sealed class Env
        {
            Dictionary<string, Typ> types = new Dictionary<string, Typ>();
            Dictionary<string, TypFun> fns = new Dictionary<string, TypFun>();

            /// <summary>
            /// Resolve a fully-qualified type name to a final type definition.
            /// If no internal type exists, it defaults to external linkage,
            /// meaning it is a predefined type that will not be filled from a table.
            /// </summary>
            /// <param name="name"></param>
            /// <returns></returns>
            internal Typ Resolve(string name)
            {
                if (!types.ContainsKey(name)) types[name] = new TypExtern(name);
                return types[name];
            }
            /// <summary>
            /// Creates a type variable, ie. a deferred type binding, if binding
            /// does not already exist.
            /// </summary>
            /// <param name="name"></param>
            /// <returns></returns>
            public Typ Var(string name)
            {
                return types.ContainsKey(name)
                     ? types[name]
                     : new TypVar(this, name);
            }
            /// <summary>
            /// Declare a new InteralType, meaning a type that will be filled in from table data.
            /// </summary>
            /// <param name="name"></param>
            /// <returns></returns>
            public TypIntern Define(string name)
            {
                if (!types.ContainsKey(name)) types[name] = new TypIntern(name);
                // promote to internal type on demand
                var typ = types[name];
                if (!(typ is TypIntern)) types[name] = typ = new TypIntern(name);
                return typ as TypIntern;
            }
            /// <summary>
            /// Declare a function with the given name.
            /// </summary>
            /// <param name="name"></param>
            /// <returns></returns>
            public TypFun Fun(string name)
            {
                return fns[name];
            }
            /// <summary>
            /// Declare a function with the given name and argument and return types.
            /// </summary>
            /// <param name="name"></param>
            /// <param name="arg"></param>
            /// <param name="returnType"></param>
            /// <returns></returns>
            public TypFun Fun(string name, TypIntern arg, Typ returnType)
            {
                if (!fns.ContainsKey(name))
                {
                    var fn = new TypFun(arg, returnType);
                    arg.Functions.Add(name, fn);
                    fns[name] = fn;
                }
                return Fun(name);
            }
            /// <summary>
            /// We're generally only interested in internal types.
            /// </summary>
            public IEnumerable<TypIntern> Types
            {
                get
                {
                    foreach (var type in types.Values)
                    {
                        if (type is TypIntern)
                        {
                            yield return type as TypIntern;
                        }
                    }
                }
            }
            /// <summary>
            /// The set of functions to be generated.
            /// </summary>
            public IEnumerable<TypFun> Functions
            {
                get { return fns.Values; }
            }


            /// <summary>
            /// Base class for all types.
            /// </summary>
            public abstract class Typ
            {
                internal Typ(string fqn)
                {
                    FQN = fqn;
                }
                /// <summary>
                /// Fully-qualified type name.
                /// </summary>
                public string FQN { get; private set; }
                public string Namespace()
                {
                    var i = FQN.LastIndexOf('.');
                    return i < 0 ? "" : FQN.Substring(0, i);
                }
                /// <summary>
                /// Return the non-fully qualified name of the type.
                /// </summary>
                /// <returns></returns>
                public string BaseType()
                {
                    var i = FQN.LastIndexOf('.') + 1;
                    return FQN.Substring(i <= 0 ? 0 : i);
                }
                public override string ToString()
                {
                    return FQN;
                }
                /// <summary>
                /// Resolve the Typ to a ground Typ, ie. either internal or external.
                /// </summary>
                /// <param name="mappings"></param>
                /// <param name="env"></param>
                /// <returns></returns>
                public virtual Typ Resolve(Dictionary<string, TableMapping> mappings, Env env)
                {
                    return this;
                }
            }
            /// <summary>
            /// An unresolved type reference.
            /// </summary>
            public sealed class TypVar : Typ
            {
                public TypVar(Env env, string fqn) : base(fqn)
                {
                    Env = env;
                }
                internal Typ Underlying { get; private set; }
                internal Env Env { get; private set; }
                public override Typ Resolve(Dictionary<string, TableMapping> mappings, Env env)
                {
                    if (Underlying != null) return Underlying;
                    var table = mappings.Values.Where(t => t.EnumFQN == FQN).SingleOrDefault();
                    if (table != null) Compile(table, mappings, env);
                    return Underlying = Env.Resolve(FQN);
                }
            }
            /// <summary>
            /// External resolved type.
            /// </summary>
            public sealed class TypExtern : Typ
            {
                public TypExtern(string name) : base(name) { }
            }
            /// <summary>
            /// Internal resolved type.
            /// </summary>
            public sealed class TypIntern : Typ
            {
                internal TypIntern(string name)
                    : base(name)
                {
                    Values = new Dictionary<string, string>();
                    Functions = new Dictionary<string, TypFun>();
                }
                /// <summary>
                /// Map field name to value.
                /// </summary>
                public Dictionary<string, string> Values { get; private set; }
                /// <summary>
                /// Set of functions for this type.
                /// </summary>
                internal Dictionary<string, TypFun> Functions { get; private set; }
            }
            /// <summary>
            /// Functions on types.
            /// </summary>
            public sealed class TypFun
            {
                internal TypFun(Typ arg, Typ returnType)
                {
                    ArgType = arg;
                    ReturnType = returnType;
                    Cases = new Dictionary<string, string>();
                }
                /// <summary>
                /// The input type of the function.
                /// </summary>
                public Typ ArgType { get; private set; }
                /// <summary>
                /// The return type of the function.
                /// </summary>
                public Typ ReturnType { get; private set; }
                /// <summary>
                /// Maps ArgType.FieldName to a return expression.
                /// </summary>
                public Dictionary<string, string> Cases { get; private set; }
            }
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
            var config = File.ReadAllLines("../../config.txt");
            try
            {
                var env = new Env();
                var cfg = Parse(config, args);
                using (var conn = GetConn(cfg.DbType, cfg.DB))
                {
                    conn.Open();
                    foreach (var table in cfg.Mappings.Values)
                    {
                        Schema(table, conn);
                    }
                }
                foreach (var table in cfg.Mappings.Values)
                {
                    Compile(table, cfg.Mappings, env);
                }
                Output(env, cfg.OutputFile, cfg.Namespaces);
                Console.WriteLine("Mapping successfully generated...");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Console.ReadLine();
            }
        }
        #region Output function
        /// <summary>
        /// Write out the generated program as a C# file.
        /// </summary>
        /// <param name="env"></param>
        /// <param name="file"></param>
        /// <param name="namespaces"></param>
        static void Output(Env env, string file, List<string> namespaces)
        {
            if (File.Exists(file)) File.Delete(file);
            using (var op = File.CreateText(file))
            {
                // included namespaces
                op.WriteLine("using System;");
                foreach (var ns in namespaces) op.WriteLine("using {0};", ns);

                // write out enum declaration with its stub
                foreach (var type in env.Types)
                {
                    // enum namespace
                    var ns = type.Namespace();
                    if (!string.IsNullOrEmpty(ns))
                    {
                        op.WriteLine("namespace {0}", ns);
                        op.WriteLine('{');
                    }
                    // write out enum definition
                    op.WriteLine("public enum {0}", type.BaseType());
                    op.WriteLine('{');
                    foreach (var value in type.Values)
                    {
                        op.WriteLine("        {0} = {1},", value.Key, value.Value);
                    }
                    op.WriteLine('}');

                    // write out any column functions, if applicable
                    if (type.Functions.Count > 0)
                    {
                        op.WriteLine("public static class {0}Extensions", type.BaseType());
                        op.WriteLine('{');
                        foreach (var fn in type.Functions)
                        {
                            // each function is an extension method which switches
                            // on the enum value and maps it to another value

                            op.WriteLine("    public static {0} {1}(this {2} value)",
                                         fn.Value.ReturnType, fn.Key, fn.Value.ArgType.BaseType());
                            op.WriteLine("    {");
                            op.WriteLine("        switch (value)");
                            op.WriteLine("        {");
                            foreach (var _case in fn.Value.Cases)
                            {
                                op.WriteLine("            case {0}.{1}: return {2};",
                                             fn.Value.ArgType.BaseType(), _case.Key, _case.Value);
                            }
                            op.WriteLine("            default: throw new ArgumentException(\"Invalid {0} provided.\");",
                                         type.BaseType());
                            op.WriteLine("        }");
                            op.WriteLine("    }");
                        }
                        op.WriteLine('}');
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
        static string quote(object val, Type expected)
        {
            return val == null   ? "default(" + expected.FullName + ")":
                   val is string ? "\"" + (val as string) + "\"":
                   val is bool   ? val.ToString().ToLower():
                                   val.ToString();
        }
        /// <summary>
        /// Normalize the given string as a valid C# identifier.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        static string normalize(this string name)
        {
            var sb = new StringBuilder();
            var ws = true;
            foreach (var c in name)
            {
                if (char.IsWhiteSpace(c))
                {
                    ws = true;
                    continue;
                }
                sb.Append(ws && char.IsLower(c) ? char.ToUpper(c) : c);
                ws = false;
            }
            return sb.ToString();
        }
        /// <summary>
        /// Perform a value or template substitution, depending on the given args.
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="col"></param>
        /// <param name="value"></param>
        /// <param name="expectedType"></param>
        /// <returns></returns>
        static string substitute(string expression, string col, string value, Env.Typ expectedType)
        {
            return string.IsNullOrEmpty(expression) ? value:
                   string.IsNullOrEmpty(value)      ? "default(" + expectedType + ")":
                                                      expression.Replace("%" + col + "%", value);
        }
        #region Database Schema
        static void Schema(TableMapping table, DbConnection conn)
        {
            var cmd = conn.CreateCommand();
                cmd.CommandText = string.Format(
                    "SELECT {0} FROM {1}",
                    table.PK.Cons(table.Name.Cons(table.Columns.Keys)).Format(","), table.Table);

            // load all table values into TableData and DataRow
            using (var reader = cmd.ExecuteReader(CommandBehavior.KeyInfo))
            {
                var data = new TableData();
                BindColumns(data, table, reader);
                BindRows(data, table, reader);
            }
        }
        static void BindColumns(TableData data, TableMapping table, DbDataReader reader)
        {
            // load column definitions, ie. names and types, starting with
            // PK and name column
            data.Columns[table.PK] = reader.GetFieldType(0);
            data.Columns[table.Name] = reader.GetFieldType(1);
            table.Data = data;
            foreach (var col in table.Columns)
            {
                var i = reader.GetOrdinal(col.Key);
                data.Columns[col.Key] = reader.GetFieldType(i);
            }
        }
        static void BindRows(TableData data, TableMapping table, DbDataReader reader)
        {
            // load row data, including PK and name columns
            foreach (IDataRecord drow in reader)
            {
                var row = new RowData();
                    row.Cells[table.PK] = drow.GetValue(0);
                    row.Cells[table.Name] = drow.GetValue(1);
                data.Rows.Add(row);
                foreach (var col in table.Columns)
                {
                    var i = drow.GetOrdinal(col.Key);
                    row.Cells[col.Key] = drow.GetValue(i);
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
        static void Compile(TableMapping table, Dictionary<string, TableMapping> mappings, Env env)
        {
            // declare top-level type
            var type = env.Define(table.EnumFQN);

            // build function signatures with explicitly typed
            // argument and return types
            foreach (var col in table.Columns)
            {
                // if return type provided, use that, else use type
                // of table column
                var returnType = string.IsNullOrEmpty(col.Value.ReturnType)
                               ? table.Data.Columns[col.Key].FullName
                               : col.Value.ReturnType;
                // by default, ReturnType is TypVar, ie. an unresolved type
                // the concrete type is resolved on-demand
                env.Fun(col.Value.Function, type, env.Var(returnType));
            }
            Functions(table, mappings, env);
        }
        /// <summary>
        /// Compile the given table's functions with the given environment and table mappings.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="mappings"></param>
        /// <param name="env"></param>
        static void Functions(TableMapping table, Dictionary<string, TableMapping> mappings, Env env)
        {
            var type = env.Define(table.EnumFQN);
            var data = table.Data;

            // fill in enum and switch stubs
            foreach (var row in data.Rows)
            {
                // extract primary key value and description
                var pk = quote(row.Cells[table.PK], data.Columns[table.PK]);
                var pkname = normalize(row.Cells[table.Name].ToString());
                type.Values[pkname] = pk;

                // fill in switch-statement stubs
                foreach (var col in table.Columns)
                {
                    var fn = env.Fun(col.Value.Function);
                    // ensure Typ is fully resolved to a ground type
                    var returnedType = fn.ReturnType.Resolve(mappings, env);
                    var fk = quote(row.Cells[col.Key], data.Columns[col.Key]);
                    string exp;
                    // if internal type, resolve foreign key value to enum name
                    // else perform an expression substitution
                    if (returnedType is Env.TypIntern)
                    {
                        // checks whether the fk value has materialized yet
                        // if not, it compiles it before continuing
                        var rt = returnedType as Env.TypIntern;
                        var fkname = rt.Values.Where(v => v.Value == fk)
                                              .Select(v => v.Key)
                                              .Single();
                        // if returned type shares a namespace with arg type,
                        // then remove the shared part of the path
                        var ns = rt.Namespace();
                        var qn = type.FQN.StartsWith(ns) ? rt.FQN.Substring(ns.Length + 1) : rt.FQN;
                        exp = string.Format("{0}.{1}", qn, fkname);
                    }
                    else
                    {
                        // substitute escaped field value for quoted column name
                        exp = substitute(col.Value.Expression, col.Key, fk, returnedType);
                    }
                    fn.Cases[pkname] = exp;
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
        static string[] split(string token, string input)
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
        /// Search the given config values by key, and return otherwise if key not found.
        /// </summary>
        /// <param name="args"></param>
        /// <param name="key"></param>
        /// <param name="otherwise"></param>
        /// <returns></returns>
        static string FindByKey(this string[] args, string key, string otherwise)
        {
            var v = from a in args
                    where a.StartsWith(key, StringComparison.InvariantCultureIgnoreCase)
                       && !char.IsLetterOrDigit(a[key.Length])
                    let val = split("=", a)
                    select val[1].Trim();
            var result = v.FirstOrDefault() ?? otherwise;
            if (result == null) throw new ArgumentException("Missing " + key);
            return result;
        }
        /// <summary>
        /// Throw a parse error with line number info.
        /// </summary>
        /// <param name="cond"></param>
        /// <param name="err"></param>
        /// <param name="line"></param>
        static void error(bool cond, string err, int line)
        {
            if (cond) throw new ArgumentException(string.Format("ERROR: line {0}, {1}.", line+1, err));
        }
        /// <summary>
        /// Parse the mapping files and command-line arguments.
        /// </summary>
        /// <param name="config"></param>
        /// <param name="args"></param>
        /// <returns></returns>
        static Config Parse(string[] config, string[] args)
        {
            var map = new Dictionary<string, TableMapping>();
            var cfg = new Config { Namespaces = new List<string>(), Mappings = map };
            // parse command-line arguments
            foreach (var a in args)
            {
                if (a.StartsWith("/out:"))
                {
                    cfg.OutputFile = a.Substring("/out:".Length);
                }
            }
            // parse mapping file
            for (var i = 0; i < config.Length; ++i)
            {
                if (config[i].StartsWith("::database"))
                {
                    // declare the database type and connection string
                    var line = config[i].Split('=');
                    error(line.Length < 2, "Improper ::database declaration", i);
                    var dbType = line[1].Trim();
                    cfg.DbType = dbType;
                    error(++i >= config.Length, "Missing ::database connection string", i);
                    cfg.DB = config[i].Trim();
                }
                else if (config[i].StartsWith("::using"))
                {
                    // included namespaces in output file
                    var line = config[i].Split('=');
                    error(line.Length < 2, "Improper ::using declaration", i);
                    var ns = line[1].Trim();
                    cfg.Namespaces.Add(ns);
                }
                else if (config[i].StartsWith("::table"))
                {
                    // extract top-level table info, ie. pk, enum name, etc.
                    var table = split("::", config[i]);
                    error(table.Length < 4, "::table requires table, enum, pk, and name specified.", i);
                    var tname = table.FindByKey("table", null);
                    var tableMap = new TableMapping
                    {
                        Table = tname,
                        EnumFQN = table.FindByKey("enum", tname),
                        PK = table.FindByKey("pk", null),
                        Name = table.FindByKey("name", null),
                        Columns = new Dictionary<string, ColumnMapping>(),
                    };
                    // extract column info from mapping file
                    for (++i; i < config.Length && !config[i].StartsWith("::"); ++i)
                    {
                        error(string.IsNullOrEmpty(config[i]), "Missing column information for table " + tname, i);
                        var column = split("::", config[i]);
                        var name = column[0].Trim();
                        tableMap.Columns[name] = new ColumnMapping
                        {
                            Table = tableMap,
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
