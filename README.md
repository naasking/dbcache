# DbCache

This is a command-line tool to connect to a database and generate a set of
enums and extension methods on those enums that reflect the content of
those tables. For instance, the following table:

|CurrencyId|Name|Country|
|----------|----|-------|
|1|CAD|Canada|
|2|USD|USA|

Would generate the following code:

    public enum Currency
    {
        CAD = 1,
        USD = 2,
    }

    public static class Currencies
    {
        public static string Name(this Currency currency)
        {
            switch(currency)
            {
                case Currency.CAD: return "CAD";
                case Currency.USD: return "USD";
                default: throw new NotFoundException();
            }
        }

        public static string Country(this Currency currency)
        {
            switch(currency)
            {
                case Currency.CAD: return "Canada";
                case Currency.USD: return "USA";
                default: throw new NotFoundException();
            }
        }
    }

You are able to customize the output in numerous ways.

# Command

You run the command as follows:

    dbcache.exe /in:enums.txt /out:..\path\to\generated\source.cs

The enums.txt file specifies the database connection string and a description
of the tables and columns to convert to code. The format is as follows:

    ::database = SqlClient
    Data Source=.\YourDbInstance;user id=YourDbUser;password=YourDbPwd;Initial Catalog=YourDb;Connection Timeout=240
    ::using = DefaultNamespace1
    ::using = DefaultNamespace2
    ::table = FirstTable::enum = Your.Namespace.FirstTableEnum::pk = FirstTableId::name = TableColumnToUseAsEnumName
    SecondTableId::function = ToSecondTable::returnType = Your.Namespace.SecondTableEnum
    Column2
    ::table = SecondTable::enum = Your.Namespace.SecondTableEnum::pk = FirstTableId::name = TableColumnToUseAsEnumName
    Column1

The `function` and `returnType` parameters are optional, in which case the
generated extension method name will match the column name, and the return
type will match the type most closely associated with the DB column type.

The `returnType` must be castable in C# from the DB column type, eg.
typically this will be another enum type and the underlying DB type will
be some integer type that is also specified as an enum per the sample
above.

The `using` directives list any namespaces to include in the output file.