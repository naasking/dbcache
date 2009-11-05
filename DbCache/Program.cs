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
                    var fn = new TypFun(name, arg, returnType);
                    arg.Functions.Add(name, fn);
                    fns[name] = fn;
                }
                return Fun(name);
            }
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
                public string BaseType()
                {
                    var i = FQN.LastIndexOf('.') + 1;
                    return FQN.Substring(i <= 0 ? 0 : i);
                }
                public override string ToString()
                {
                    return FQN;
                }
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
                internal TypFun(string name, Typ arg, Typ returnType)
                {
                    FunctionName = name;
                    ArgType = arg;
                    ReturnType = returnType;
                    Cases = new Dictionary<string, string>();
                }
                public string FunctionName { get; private set; }
                public Typ ArgType { get; private set; }
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
        // 2. construct SELECT query
        // 3. extract table schema from data reader
        // 4. output enum + static class with enum extension methods
        static void Main(string[] args)
        {
            var config = File.ReadAllLines("../../config.txt");
            try
            {
                var env = new Env();
                var cfg = Parse(config);
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
                Output(env, "out.cs", cfg.Namespaces);
                Console.WriteLine("Mapping successfully generated...");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Console.ReadLine();
            }
        }
        #region Output function
        static void Output(Env env, string file, List<string> namespaces)
        {
            if (File.Exists(file)) File.Delete(file);
            using (var op = File.CreateText(file))
            {
                op.WriteLine("using System;");
                foreach (var ns in namespaces) op.WriteLine("using {0};", ns);

                // write out enum declaration with its stub
                foreach (var type in env.Types)
                {
                    var ns = type.Namespace();
                    if (!string.IsNullOrEmpty(ns))
                    {
                        op.WriteLine("namespace {0}", ns);
                        op.WriteLine('{');
                    }
                    op.WriteLine("public enum {0}", type.BaseType());
                    op.WriteLine('{');
                    foreach (var value in type.Values)
                    {
                        op.WriteLine("        {0} = {1},", value.Key, value.Value);
                    }
                    op.WriteLine('}');

                    if (type.Functions.Count > 0)
                    {
                        op.WriteLine("public static class {0}Extensions", type.BaseType());
                        op.WriteLine('{');
                        foreach (var fn in type.Functions.Values)
                        {
                            op.WriteLine("    public static {0} {1}(this {2} value)",
                                         fn.ReturnType, fn.FunctionName, fn.ArgType.BaseType());
                            op.WriteLine("    {");
                            op.WriteLine("        switch (value)");
                            op.WriteLine("        {");
                            foreach (var _case in fn.Cases)
                            {
                                op.WriteLine("            case {0}.{1}: return {2};",
                                             fn.ArgType.BaseType(), _case.Key, _case.Value);
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
        static string quote(object val, Type expected)
        {
            return val == null   ? "default(" + expected.FullName + ")":
                   val is string ? "\"" + (val as string) + "\"":
                   val is bool   ? val.ToString().ToLower():
                                   val.ToString();
        }
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
            data.Columns.Add(table.PK, reader.GetFieldType(0));
            data.Columns.Add(table.Name, reader.GetFieldType(1));
            table.Data = data;
            foreach (var col in table.Columns)
            {
                var i = reader.GetOrdinal(col.Key);
                data.Columns.Add(col.Key, reader.GetFieldType(i));
            }
        }
        static void BindRows(TableData data, TableMapping table, DbDataReader reader)
        {
            // load row data, including PK and name columns
            foreach (IDataRecord drow in reader)
            {
                var row = new RowData();
                    row.Cells.Add(table.PK, drow.GetValue(0));
                    row.Cells.Add(table.Name, drow.GetValue(1));
                data.Rows.Add(row);
                foreach (var col in table.Columns)
                {
                    var i = drow.GetOrdinal(col.Key);
                    row.Cells.Add(col.Key, drow.GetValue(i));
                }
            }
        }
        #endregion
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
                env.Fun(col.Value.Function, type, env.Var(returnType));
            }
            Functions(table, mappings, env);
        }
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
                    fn.Cases.Add(pkname, exp);
                }
            }
        }
        #region Parsing functions
        static string[] split(string token, string input)
        {
            return input.Split(StringSplitOptions.RemoveEmptyEntries, token);
        }
        static DbConnection GetConn(string connType, string db)
        {
            switch (connType)
            {
                case "SqlClient": return new SqlConnection(db);
                default:          return new OleDbConnection(db);
            }
        }
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
        static void error(bool cond, string err, int line)
        {
            if (cond) throw new ArgumentException(string.Format("ERROR: line {0}, {1}.", line+1, err));
        }
        static Config Parse(string[] config)
        {
            var map = new Dictionary<string, TableMapping>();
            var cfg = new Config { Namespaces = new List<string>(), Mappings = map };
            for (var i = 0; i < config.Length; ++i)
            {
                if (config[i].StartsWith("::database"))
                {
                    var line = config[i].Split('=');
                    error(line.Length < 2, "Improper ::database declaration", i);
                    var dbType = line[1].Trim();
                    cfg.DbType = dbType;
                    error(++i >= config.Length, "Missing ::database connection string", i);
                    cfg.DB = config[i].Trim();
                }
                else if (config[i].StartsWith("::using"))
                {
                    var line = config[i].Split('=');
                    error(line.Length < 2, "Improper ::using declaration", i);
                    var ns = line[1].Trim();
                    cfg.Namespaces.Add(ns);
                }
                else if (config[i].StartsWith("::table"))
                {
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
                    for (++i; i < config.Length && !config[i].StartsWith("::"); ++i)
                    {
                        //if (config[i].StartsWith("--")) continue; // skip comments
                        error(string.IsNullOrEmpty(config[i]), "Missing column information for table " + tname, i);
                        var column = split("::", config[i]);
                        var name = column[0].Trim();
                        tableMap.Columns.Add(name, new ColumnMapping
                        {
                            Table = tableMap,
                            Function = column.FindByKey("function", name),
                            Expression = column.FindByKey("expression", ""),
                            ReturnType = column.FindByKey("returnType", ""),
                        });
                    }
                    --i; // undo the last increment after processing columns
                    map.Add(tableMap.EnumFQN, tableMap);
                }
            }
            return cfg;
        }
        #endregion
    }
}
