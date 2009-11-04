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
    //FIXME: need to properly name enum values (need another config value?)
    //FIXME: need to use enum values in switch expressions
    //FIXME: need to throw exceptions for out of bound inputs
    //FIXME: need to return default(E) for null field values
    class Program
    {
        struct TableMapping
        {
            public string Table { get; set; }
            public string Enum { get; set; }
            public Dictionary<string, ColumnMapping> Columns { get; set; }
        }
        struct ColumnMapping
        {
            public string Column { get; set; }
            public string Function { get; set; }
            public string Expression { get; set; }
            public string TypeName { get; set; }
        }
        struct Output
        {
            public StringBuilder Enum { get; set; }
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
            if (!config[0].StartsWith("::database")) throw new ArgumentException("Config file must start with ::database connection string.");
            var dbType = split("::", config[0]);
            if (dbType.Length < 2) throw new ArgumentException("Please specify ::database::DbType, where DbType = SqlClient, OleDb, etc.");
            if (string.IsNullOrEmpty(config[1])) throw new ArgumentException("Missing database connection string.");
            var db = config[1];
            var tables = Read(config);
            using (var conn = GetConn(dbType[1], db))
            {
                conn.Open();
                foreach (var o in MapTables(tables, conn))
                {
                    File.AppendAllText("out.cs", o.Enum.ToString());
                    foreach (var ext in o.Extensions)
                    {
                        File.AppendAllText("out.cs", ext.Value.ToString());
                    }
                }
                File.AppendAllText("out.cs", "}\n");
            }
            //Console.ReadLine();
        }
        static string quote(object val)
        {
            return val is string ? "\"" + (val as string) + "\"":
                                   val.ToString();
        }
        static IEnumerable<Output> MapTables(IEnumerable<TableMapping> tables, DbConnection conn)
        {
            foreach (var t in tables)
            {
                var op = new Output { Enum = new StringBuilder(), Extensions = new Dictionary<ColumnMapping, StringBuilder>() };
                    op.Enum.AppendFormat("using System;\n\npublic enum {0}\n{{\n", t.Enum);

                var cols = t.Columns.Aggregate("", (acc, col) => string.IsNullOrEmpty(acc) ? col.Key : acc + ", " + col.Key);
                var cmd = conn.CreateCommand();
                    cmd.CommandText = string.Format("SELECT {0} FROM {1}", cols, t.Table);
                var reader = cmd.ExecuteReader(CommandBehavior.KeyInfo);
                // skip PK by starting at 1
                for (int i = 1; i < reader.FieldCount; ++i)
                {
                    var col = t.Columns[reader.GetName(i)];
                    var sb = new StringBuilder();
                        sb.AppendFormat(@"
    public static {0} {1}(this {2} value)
    {{
        switch(value)
        {{
", string.IsNullOrEmpty(col.TypeName) ? reader.GetFieldType(i).Name : col.TypeName,
   col.Function, t.Enum);
                        op.Extensions.Add(col, sb);
                }
                // assume PK is at index 0
                foreach (IDataRecord row in reader)
                {
                    var enumType = row.GetFieldType(0);
                    var val = quote(row.GetValue(0));
                    op.Enum.AppendFormat("    {0} = {1},\n", row.GetName(0), val);
                    for (int i = 1; i < row.FieldCount; ++i)
                    {
                        var col = t.Columns[row.GetName(i)];
                        var colVal = quote(row.GetValue(i));
                        // substitute escaped field value for quoted column name
                        var exp = string.IsNullOrEmpty(col.Expression)
                                ? colVal
                                : col.Expression.Replace("%" + col.Column + "%", colVal);
                        var sb = op.Extensions[col];
                            sb.AppendFormat("        case {0}: return {1};\n", val, exp);
                    }
                }
                op.Enum.AppendLine("}\npublic static class " + t.Enum + "Extensions\n{");
                foreach (var col in op.Extensions.Values) col.AppendLine("        }\n    }");
                yield return op;
            }
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
        static IEnumerable<TableMapping> Read(string[] config)
        {
            for (var i = 1; i < config.Length; ++i)
            {
                if (config[i].StartsWith("::table::"))
                {
                    var tableConfig = split(" as ", config[i].Substring("::table::".Length));
                    var tableMap = new TableMapping
                    {
                        Table = tableConfig[0],
                        Enum = tableConfig.Length > 1 ? tableConfig[1] : tableConfig[0],
                        Columns = new Dictionary<string,ColumnMapping>(),
                    };
                    for (var j = ++i; j < config.Length && !config[j].StartsWith("::"); ++j, ++i)
                    {
                        var values = split("::", config[j]);
                        var map = split(" as ", values[0]);
                        tableMap.Columns.Add(map[0], new ColumnMapping
                        {
                            Column = map[0],
                            Function = map.Length > 1 ? map[1] : map[0],
                            Expression = values.Length > 1 ? values[1] : "",
                            TypeName = values.Length > 2 ? values[2] : "",
                        });
                    }
                    yield return tableMap;
                }
            }
        }
        //static void DisplayData(DataTable table)
        //{
        //    foreach (DataRow row in table.Rows)
        //    {
        //        foreach (DataColumn col in table.Columns)
        //        {
        //            Console.WriteLine("{0} = {1}", col.ColumnName, row[col]);
        //        }
        //        Console.WriteLine("============================");
        //    }
        //}
    }
}
