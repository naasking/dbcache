using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Data.OleDb;

namespace DbCache
{
    static class Program
    {
        /// <summary>
        /// A table to enum mapping declaration.
        /// </summary>
        sealed class TableMapping
        {
            public string Table { get; set; }
            public string Enum { get; set; }
            public string Name { get; set; }
            public string PK { get; set; }
            public Dictionary<string, ColumnMapping> Columns { get; set; }
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
        /// Tracks intermediate stubs for the enum and all dependent extension methods.
        /// </summary>
        struct Output
        {
            /// <summary>
            /// Stub for enum.
            /// </summary>
            public StringBuilder Enum { get; set; }
            /// <summary>
            /// Map of columns to stubs for extension methods.
            /// </summary>
            public Dictionary<ColumnMapping, StringBuilder> Extensions { get; set; }
        }

        // Extract set of named tables into set of enums with appropriate extension methods
        // mapping primary keys to the respective rows.
        // 1. extract table mappings
        // 2. construct SELECT query
        // 3. extract table schema from data reader
        // 4. output enum + static class with enum extension methods
        //
        // Config: map table=>enum, map column=>(function name, function map, return type)
        //FIXME: should lookup FK constraints?
        static void Main(string[] args)
        {
            var config = File.ReadAllLines("../../config.txt");
            //if (!config[0].StartsWith("::database")) throw new ArgumentException("Config file must start with ::database connection string.");
            var dbType = config.FindByKey("::database", null);
            //if (dbType.Length < 1) throw new ArgumentException("Please specify ::database = DbType, where DbType = SqlClient, OleDb, etc.");
            if (string.IsNullOrEmpty(config[1])) throw new ArgumentException("Missing database connection string.");
            var db = config[1];
            var tables = Read(config);
            using (var conn = GetConn(dbType, db))
            {
                conn.Open();
                if (File.Exists("out.cs")) File.Delete("out.cs");
                using (var op = File.CreateText("out.cs"))
                {
                    op.WriteLine("using System;");
                    // write out enum declaration with its stub
                    foreach (var table in tables)
                    {
                        var stub = MapTable(table, conn);
                        op.WriteLine("public enum {0}", table.Enum.ToString());
                        op.WriteLine('{');
                        op.Write(stub.Enum.ToString());
                        op.WriteLine('}');

                        op.WriteLine("public static class {0}Extensions", table.Enum);
                        op.WriteLine('{');
                        foreach (var col in table.Columns.Values)
                        {
                            var map = stub.Extensions[col];
                            op.WriteLine("    public static {0} {1}(this {2} value)",
                                         col.ReturnType, col.Function, table.Enum);
                            op.WriteLine("    {");
                            op.WriteLine("        switch (value)");
                            op.WriteLine("        {");
                            op.Write(map.ToString());
                            op.WriteLine("            default: throw new ArgumentException(\"Invalid {0} provided.\");", table.Enum);
                            op.WriteLine("        }");
                            op.WriteLine("    }");
                        }
                        op.WriteLine('}');
                    }
                }
            }
        }
        static string quote(object val, Type expected)
        {
            return val == null   ? "default(" + expected.Name + ")":
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
        static string substitute(string expression, string col, string value, string expectedType)
        {
            return string.IsNullOrEmpty(value)
                ? "default(" + expectedType + ")"
                : expression.Replace("%" + col + "%", value);
        }
        static Output MapTable(TableMapping t, DbConnection conn)
        {
            var op = new Output
            {
                Enum = new StringBuilder(),
                Extensions = new Dictionary<ColumnMapping, StringBuilder>()
            };
            
            var cols = t.Columns.Aggregate("", (acc, col) => string.IsNullOrEmpty(acc) ? col.Key : acc + ", " + col.Key);
            var cmd = conn.CreateCommand();
            cmd.CommandText = string.Format("SELECT {0}, {1}, {2} FROM {3}", t.PK, t.Name, cols, t.Table);
            var reader = cmd.ExecuteReader(CommandBehavior.KeyInfo);

            // build column stubs and update return type based on
            // column information, if not explicitly provided
            foreach (var col in t.Columns.Values)
            {
                var i = reader.GetOrdinal(col.Column);
                if (string.IsNullOrEmpty(col.ReturnType))
                {
                    col.ReturnType = reader.GetFieldType(i).Name;
                }
                op.Extensions.Add(col, new StringBuilder());
            }
            // fill in enum and switch stubs
            foreach (IDataRecord row in reader)
            {
                var pk = row.GetOrdinal(t.PK);
                var enumType = row.GetDataTypeName(pk);
                var val = quote(row.GetValue(pk), row.GetFieldType(pk));
                var name = normalize(row.GetValue(row.GetOrdinal(t.Name)).ToString());
                op.Enum.AppendFormat("    {0} = {1},{2}", name, val, Environment.NewLine);

                // fill in switch stubs
                foreach (var col in t.Columns.Values)
                {
                    var i = reader.GetOrdinal(col.Column);
                    var colVal = quote(row.GetValue(i), row.GetFieldType(i));
                    // substitute escaped field value for quoted column name
                    var exp = string.IsNullOrEmpty(col.Expression)
                            ? colVal
                            : substitute(col.Expression, col.Column, colVal, col.ReturnType);
                    var sb = op.Extensions[col];
                    sb.AppendFormat("            case {0}.{1}: return {2};{3}", t.Enum, name, exp, Environment.NewLine);
                }
            }
            return op;
        }
        static string[] split(string token, string input)
        {
            return input.Split(new string[] { token }, StringSplitOptions.RemoveEmptyEntries);
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
                    let val = split("=", a)
                    select val[1].Trim();
            var result = v.SingleOrDefault() ?? otherwise;
            if (result == null) throw new ArgumentException("Missing " + key);
            return result;
        }
        static IEnumerable<TableMapping> Read(string[] config)
        {
            for (var i = 1; i < config.Length; ++i)
            {
                if (config[i].StartsWith("::table"))
                {
                    var table = split("::", config[i]);
                    if (table.Length < 4) throw new InvalidOperationException("::table requires table, enum, pk, and name specified.");
                    var tname = table.FindByKey("table", null);
                    var tableMap = new TableMapping
                    {
                        Table = tname,
                        Enum = table.FindByKey("enum", tname),
                        PK = table.FindByKey("pk", null),
                        Name = table.FindByKey("name", null),
                        Columns = new Dictionary<string,ColumnMapping>(),
                    };
                    for (var j = ++i; j < config.Length && !config[j].StartsWith("::"); ++j, ++i)
                    {
                        var column = split("::", config[j]);
                        tableMap.Columns.Add(column[0], new ColumnMapping
                        {
                            Table = tableMap,
                            Column = column[0],
                            Function = column.FindByKey("function", column[0]),
                            Expression = column.FindByKey("expression", ""),
                            ReturnType = column.FindByKey("returnType", ""),
                        });
                    }
                    yield return tableMap;
                }
            }
        }
    }
}
