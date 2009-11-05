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
            public TableMapping Table { get; set; }
            public string Column { get; set; }
            public string Function { get; set; }
            public string Expression { get; set; }
            public string ReturnType { get; set; }
        }
        /// <summary>
        /// Encapsulates all column and row data of a given table.
        /// </summary>
        sealed class TableData
        {
            public TableData(string name)
            {
                Name = name;
                Rows = new List<RowData>();
                Columns = new Dictionary<string, Type>();
            }
            public string Name { get; private set; }
            public Dictionary<string, Type> Columns { get; private set; }
            public List<RowData> Rows { get; private set; }
        }
        /// <summary>
        /// Encapsulates all data for a row in a given table.
        /// </summary>
        sealed class RowData
        {
            public RowData()
            {
                Cells = new Dictionary<string, object>();
            }
            public Dictionary<string, object> Cells { get; private set; }
        }
        /// <summary>
        /// Top-level environment.
        /// </summary>
        sealed class Env
        {
            Dictionary<string, CompiledType> types = new Dictionary<string, CompiledType>();
            Dictionary<string, CompiledFunction> fns = new Dictionary<string, CompiledFunction>();
            public CompiledType Resolve(string name)
            {
                if (!types.ContainsKey(name)) types[name] = new CompiledType(name);
                return types[name];
            }
            public InternalType Define(string name)
            {
                if (!types.ContainsKey(name)) types[name] = new InternalType(name);
                // promote to internal type on demand
                if (!(types[name] is InternalType)) types[name] = types[name].Intern();
                return types[name] as InternalType;
            }
            public CompiledFunction Fun(string name)
            {
                return fns[name];
            }
            public CompiledFunction Fun(string name, InternalType arg, CompiledType returnType)
            {
                if (!fns.ContainsKey(name))
                {
                    var fn = new CompiledFunction(name, arg, returnType);
                    arg.Functions.Add(name, fn);
                    fns[name] = fn;
                }
                return Fun(name);
            }
            public IEnumerable<InternalType> Types
            {
                get
                {
                    foreach (var type in types.Values)
                    {
                        if (type is InternalType)
                        {
                            yield return type as InternalType;
                        }
                    }
                }
            }
            public IEnumerable<CompiledFunction> Functions
            {
                get { return fns.Values; }
            }

            public class CompiledType
            {
                internal CompiledType(string fqn)
                {
                    FQN = fqn;
                }
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
                internal InternalType Intern()
                {
                    return new InternalType(FQN);
                }
            }
            public sealed class InternalType : CompiledType
            {
                internal InternalType(string name)
                    : base(name)
                {
                    Values = new Dictionary<string, string>();
                    Functions = new Dictionary<string, CompiledFunction>();
                }
                /// <summary>
                /// Map field name to value.
                /// </summary>
                public Dictionary<string, string> Values { get; private set; }
                /// <summary>
                /// Set of functions for this type.
                /// </summary>
                internal Dictionary<string, CompiledFunction> Functions { get; private set; }
            }
            public sealed class CompiledFunction
            {
                internal CompiledFunction(string name, CompiledType arg, CompiledType returnType)
                {
                    FunctionName = name;
                    ArgType = arg;
                    ReturnType = returnType;
                    Cases = new Dictionary<string, string>();
                }
                public string FunctionName { get; private set; }
                public CompiledType ArgType { get; private set; }
                public CompiledType ReturnType { get; private set; }
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
                TopLevel(cfg.Mappings, env);
                foreach (var table in cfg.Mappings.Values)
                {
                    Functions(table, cfg.Mappings, env);
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
                    if (!string.IsNullOrEmpty(ns))
                    {
                        op.WriteLine('}');
                    }
                    op.WriteLine('}');
                }
            }
        }
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
        static string substitute(string expression, string col, string value, Env.CompiledType expectedType)
        {
            return string.IsNullOrEmpty(expression) ? value:
                   string.IsNullOrEmpty(value)      ? "default(" + expectedType + ")":
                                                      expression.Replace("%" + col + "%", value);
        }
        static void Schema(TableMapping table, DbConnection conn)
        {
            var cmd = conn.CreateCommand();
            cmd.CommandText = string.Format(
                "SELECT {0}, {1}, {2} FROM {3}",
                table.PK, table.Name, table.Columns.Keys.Format(","), table.Table);

            // load all table values into TableData and DataRow
            using (var reader = cmd.ExecuteReader(CommandBehavior.KeyInfo))
            {
                // load column definitions, ie. names and types, starting with
                // PK and name column
                var data = new TableData(table.Name);
                    data.Columns.Add(table.PK, reader.GetFieldType(0));
                    data.Columns.Add(table.Name, reader.GetFieldType(1));
                table.Data = data;
                foreach (var col in table.Columns.Values)
                {
                    var i = reader.GetOrdinal(col.Column);
                    data.Columns.Add(col.Column, reader.GetFieldType(i));
                }
                // load row data, including PK and name columns
                foreach (IDataRecord drow in reader)
                {
                    var row = new RowData();
                        row.Cells.Add(table.PK, drow.GetValue(0));
                        row.Cells.Add(table.Name, drow.GetValue(1));
                    data.Rows.Add(row);
                    foreach (var col in table.Columns.Values)
                    {
                        var i = drow.GetOrdinal(col.Column);
                        row.Cells.Add(col.Column, drow.GetValue(i));
                    }
                }
            }
        }
        static void TopLevel(Dictionary<string, TableMapping> mappings, Env env)
        {
            foreach (var table in mappings.Values)
            {
                // declare top-level type
                var type = env.Define(table.EnumFQN);

                // build function signatures with explicitly typed
                // argument and return types
                foreach (var col in table.Columns.Values)
                {
                    // if return type provided, use that, else use type
                    // of table column
                    var returnType = string.IsNullOrEmpty(col.ReturnType)
                                   ? table.Data.Columns[col.Column].FullName
                                   : col.ReturnType;
                    env.Fun(col.Function, type, env.Resolve(returnType));
                }
            }
        }
        static void Functions(TableMapping table, Dictionary<string, TableMapping> mappings, Env env)
        {
            var type = env.Define(table.EnumFQN);
            var data = table.Data;

            // fill in enum and switch stubs
            foreach (var row in data.Rows)
            {
                var pk = quote(row.Cells[table.PK], data.Columns[table.PK]);
                var enumName = normalize(row.Cells[table.Name].ToString());
                type.Values.Add(enumName, pk);

                // fill in switch-statement stubs
                foreach (var col in table.Columns.Values)
                {
                    var fn = env.Fun(col.Function);
                    var returnedType = fn.ReturnType;
                    var fk = quote(row.Cells[col.Column], data.Columns[col.Column]);
                    string exp;
                    // if internal type, resolve foreign key value to enum name
                    // else perform an expression substitution
                    if (returnedType is Env.InternalType)
                    {
                        // checks whether the fk value has materialized yet
                        // if not, it compiles it before continuing
                        var rt = returnedType as Env.InternalType;
                        if (!rt.Values.ContainsKey(fk))
                        {
                            Functions(mappings.Values.Where(t => t.EnumFQN == rt.FQN).Single(),
                                      mappings, env);
                        }
                        var fkname = rt.Values[fk];
                        exp = string.Format("{0}.{1}", rt, fkname);
                    }
                    else
                    {
                        // substitute escaped field value for quoted column name
                        exp = substitute(col.Expression, col.Column, fk, returnedType);
                    }
                    fn.Cases.Add(enumName, exp);
                }
            }
        }
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
                    for (var j = ++i; j < config.Length && !config[j].StartsWith("::"); ++j, ++i)
                    {
                        if (config[j].StartsWith("--")) continue; // skip comments
                        error(string.IsNullOrEmpty(config[i]), "Missing column information for table " + tname, j);
                        var column = split("::", config[j]);
                        var name = column[0].Trim();
                        tableMap.Columns.Add(name, new ColumnMapping
                        {
                            Table = tableMap,
                            Column = name,
                            Function = column.FindByKey("function", name),
                            Expression = column.FindByKey("expression", ""),
                            ReturnType = column.FindByKey("returnType", ""),
                        });
                    }
                    map.Add(tableMap.EnumFQN, tableMap);
                }
            }
            return cfg;
        }
    }
}
