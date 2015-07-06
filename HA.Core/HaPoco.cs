using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace HA.Core
{
    // marks property as a column and optionally supplies column name
    [AttributeUsage(AttributeTargets.Property)]
    public class ColumnAttribute : Attribute
    {
        public ColumnAttribute() { }
        public ColumnAttribute(string name) { Name = name; }
        public string Name { get; set; }
    }

    // marks property as a result column and optionally supplies column name
    [AttributeUsage(AttributeTargets.Property)]
    public class ResultColumnAttribute : ColumnAttribute
    {
        public ResultColumnAttribute() { }
        public ResultColumnAttribute(string name) : base(name) { }
    }

    // Specify the table name of a poco
    [AttributeUsage(AttributeTargets.Class)]
    public class TableNameAttribute : Attribute
    {
        public TableNameAttribute(string tableName) { Value = tableName; }
        public string Value { get; private set; }
    }

    // Specific the primary key of a poco class
    [AttributeUsage(AttributeTargets.Class)]
    public class PrimaryKeyAttribute : Attribute
    {
        public PrimaryKeyAttribute(string primaryKey)
        {
            Value = primaryKey;
            AutoIncrement = true;
        }
        public string Value { get; private set; }
        public bool AutoIncrement { get; set; }
    }

    // Results from paged request
    public class Page<T>
    {
        public long CurrentPage { get; set; }
        public long TotalPages { get; set; }
        public long TotalItems { get; set; }
        public long ItemsPerPage { get; set; }
        public List<T> Items { get; set; }
        public object Context { get; set; }
    }

    // Pass as parameter value to force to DBType.AnsiString
    public class AnsiString
    {
        public AnsiString(string str) { Value = str; }
        public string Value { get; private set; }
    }

    public class TableInfo
    {
        public string TableName { get; set; }
        public string PrimaryKey { get; set; }
        public bool AutoIncrement { get; set; }
    }

    public class Database : IDisposable
    {
        string _connectionString;
        DbProviderFactory _factory;
        IDbConnection _sharedConnection;
        IDbTransaction _transaction;
        int _sharedConnectionDepth;
        int _transactionDepth;
        bool _transactionCancelled;
        string _lastSql;
        object[] _lastArgs;
        string _paramPrefix = "@";

        public virtual IDbConnection OnConnectionOpened(IDbConnection conn) { return conn; }
        public virtual void OnConnectionClosing(IDbConnection conn) { }
        public virtual void OnExecutingCommand(IDbCommand cmd) { }
        public virtual void OnExecutedCommand(IDbCommand cmd) { }
        public virtual void OnBeginTransaction() { }
        public virtual void OnEndTransaction() { }
        public virtual void OnException(Exception x)
        {
            System.Diagnostics.Debug.WriteLine(x.ToString());
            //System.Diagnostics.Debug.WriteLine(LastCommand);
        }

        public Database(string connectionStringName)
        {
            _connectionString = ConfigurationManager.ConnectionStrings[connectionStringName].ConnectionString;
            _factory = DbProviderFactories.GetFactory("System.Data.SqlClient");
        }

        public bool ForceDateTimesToUtc { get; set; }

        // Set to true to keep the first opened connection alive until this object is disposed
        public bool KeepConnectionAlive { get; set; }

        // Open a connection (can be nested)
        public void OpenSharedConnection()
        {
            if (_sharedConnectionDepth == 0)
            {
                _sharedConnection = _factory.CreateConnection();
                _sharedConnection.ConnectionString = _connectionString;
                _sharedConnection.Open();

                _sharedConnection = OnConnectionOpened(_sharedConnection);

                if (KeepConnectionAlive) 
                    _sharedConnectionDepth++;
            }
            _sharedConnectionDepth++;
        }

        // Close a previously opened connection
        public void CloseSharedConnection()
        {
            if (_sharedConnectionDepth > 0)
            {
                _sharedConnectionDepth--;
                if (_sharedConnectionDepth == 0)
                {
                    OnConnectionClosing(_sharedConnection);
                    _sharedConnection.Dispose();
                    _sharedConnection = null;
                }
            }
        }

        // Start a new transaction, can be nested, every call must be
        // matched by a call to AbortTransaction or CompleteTransaction
        // Use `using (var scope=db.Transaction) { scope.Complete(); }` to ensure correct semantics
        public void BeginTransaction()
        {
            _transactionDepth++;
            if (_transactionDepth == 1)
            {
                OpenSharedConnection();
                _transaction = _sharedConnection.BeginTransaction();
                _transactionCancelled = false;
                OnBeginTransaction();
            }
        }

        // Internal helper to cleanup transaction stuff
        void CleanupTransaction()
        {
            OnEndTransaction();

            if (_transactionCancelled) 
                _transaction.Rollback();
            else 
                _transaction.Commit();

            _transaction.Dispose();
            _transaction = null;

            CloseSharedConnection();
        }

        // Abort the entire outer most transaction scope
        public void AbortTransaction()
        {
            _transactionCancelled = true;
            if ((--_transactionDepth) == 0) 
                CleanupTransaction();
        }

        // Complete the transaction
        public void CompleteTransaction()
        {
            if ((--_transactionDepth) == 0) 
                CleanupTransaction();
        }

        // Access to our shared connection
        public IDbConnection Connection
        {
            get { return _sharedConnection; }
        }

        // Helper to create a transaction scope
        public Transaction GetTransaction()
        {
            return new Transaction(this);
        }

        // Helper to handle named parameters from object properties
        static Regex rxParams = new Regex(@"(?<!@)@\w+", RegexOptions.Compiled);
        public static string ProcessParams(string _sql, object[] args_src, List<object> args_dest)
        {
            return rxParams.Replace(_sql, m =>
            {
                string param = m.Value.Substring(1);
                object arg_val;
                int paramIndex;
                if (!int.TryParse(param, out paramIndex)) 
                    throw new ArgumentException(string.Format("Parameter '@{0}' is illegal", param));
                // Numbered parameter
                if (paramIndex < 0 || paramIndex >= args_src.Length) 
                    throw new ArgumentOutOfRangeException(string.Format("Parameter '@{0}' specified but only {1} parameters supplied (in `{2}`)", paramIndex, args_src.Length, _sql));
                arg_val = args_src[paramIndex];

                // Expand collections to parameter lists
                if ((arg_val as IEnumerable) != null && (arg_val as string) == null && (arg_val as byte[]) == null)
                {
                    var sb = new StringBuilder();
                    foreach (var i in arg_val as IEnumerable)
                    {
                        sb.Append((sb.Length == 0 ? "@" : ",@") + args_dest.Count.ToString());
                        args_dest.Add(i);
                    }
                    return sb.ToString();
                }
                else
                {
                    args_dest.Add(arg_val);
                    return "@" + (args_dest.Count - 1).ToString();
                }
            });
        }

        // Add a parameter to a DB command
        void AddParam(IDbCommand cmd, object item, string ParameterPrefix)
        {
            var p = cmd.CreateParameter();
            p.ParameterName = string.Format("{0}{1}", ParameterPrefix, cmd.Parameters.Count);
            if (item == null)
            {
                p.Value = DBNull.Value;
            }
            else
            {
                var t = item.GetType();
                if (t.IsEnum)
                {
                    p.Value = (int)item;
                }
                else if (t == typeof(Guid))
                {
                    p.Value = item.ToString();
                    p.DbType = DbType.String;
                    p.Size = 40;
                }
                else if (t == typeof(string))
                {
                    p.Size = Math.Max((item as string).Length + 1, 4000);		// Help query plan caching by using common size
                    p.Value = item;
                }
                else if (t == typeof(AnsiString))
                {
                    // Thanks @DataChomp for pointing out the SQL Server indexing performance hit of using wrong string type on varchar
                    p.Size = Math.Max((item as AnsiString).Value.Length + 1, 4000);
                    p.Value = (item as AnsiString).Value;
                    p.DbType = DbType.AnsiString;
                }
                else if (t == typeof(bool))
                {
                    p.Value = ((bool)item) ? 1 : 0;
                }
                else if (item.GetType().Name == "SqlGeography") //SqlGeography is a CLR Type
                {
                    p.GetType().GetProperty("UdtTypeName").SetValue(p, "geography", null); //geography is the equivalent SQL Server Type
                    p.Value = item;
                }
                else if (item.GetType().Name == "SqlGeometry") //SqlGeometry is a CLR Type
                {
                    p.GetType().GetProperty("UdtTypeName").SetValue(p, "geometry", null); //geography is the equivalent SQL Server Type
                    p.Value = item;
                }
                else
                {
                    p.Value = item;
                }
            }
            cmd.Parameters.Add(p);
        }

        public IDbCommand CreateCommand(IDbConnection connection, string sql, params object[] args)
        {
            // Perform named argument replacements
            var new_args = new List<object>();
            sql = ProcessParams(sql, args, new_args);
            args = new_args.ToArray();
            sql = sql.Replace("@@", "@");  // <- double @@ escapes a single @

            // Create the command and add parameters
            IDbCommand cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            cmd.Transaction = _transaction;

            foreach (var item in args)
            {
                AddParam(cmd, item, _paramPrefix);
            }

            if (!string.IsNullOrEmpty(sql)) 
                DoPreExecute(cmd);

            return cmd;
        }

        // Execute a non-query command
        public int Execute(string sql, params object[] args)
        {
            try
            {
                OpenSharedConnection();
                try
                {
                    using (var cmd = CreateCommand(_sharedConnection, sql, args))
                    {
                        var retv = cmd.ExecuteNonQuery();
                        OnExecutedCommand(cmd);
                        return retv;
                    }
                }
                finally
                {
                    CloseSharedConnection();
                }
            }
            catch (Exception x)
            {
                OnException(x);
                throw;
            }
        }

        public int Execute(Sql sql)
        {
            return Execute(sql.SQL, sql.Arguments);
        }

        // Execute and cast a scalar property
        public T ExecuteScalar<T>(string sql, params object[] args)
        {
            try
            {
                OpenSharedConnection();
                try
                {
                    using (var cmd = CreateCommand(_sharedConnection, sql, args))
                    {
                        object val = cmd.ExecuteScalar();
                        OnExecutedCommand(cmd);
                        return (T)Convert.ChangeType(val, typeof(T));
                    }
                }
                finally
                {
                    CloseSharedConnection();
                }
            }
            catch (Exception x)
            {
                OnException(x);
                throw;
            }
        }

        public T ExecuteScalar<T>(Sql sql)
        {
            return ExecuteScalar<T>(sql.SQL, sql.Arguments);
        }

        Regex rxSelect = new Regex(@"\A\s*(SELECT|EXECUTE|CALL)\s", RegexOptions.Compiled | RegexOptions.Singleline | RegexOptions.IgnoreCase | RegexOptions.Multiline);
        Regex rxFrom = new Regex(@"\A\s*FROM\s", RegexOptions.Compiled | RegexOptions.Singleline | RegexOptions.IgnoreCase | RegexOptions.Multiline);

        public int CommandTimeout { get; set; }
        public int OneTimeCommandTimeout { get; set; }

        void DoPreExecute(IDbCommand cmd)
        {
            // Setup command timeout
            if (OneTimeCommandTimeout != 0)
            {
                cmd.CommandTimeout = OneTimeCommandTimeout;
                OneTimeCommandTimeout = 0;
            }
            else if (CommandTimeout != 0)
            {
                cmd.CommandTimeout = CommandTimeout;
            }
            // Call hook
            OnExecutingCommand(cmd);
            // Save it
            _lastSql = cmd.CommandText;
            _lastArgs = (from IDataParameter parameter in cmd.Parameters select parameter.Value).ToArray();
        }

        // Automatically close one open shared connection
        public void Dispose()
        {
            // Automatically close one open connection reference
            //  (Works with KeepConnectionAlive and manually opening a shared connection)
            CloseSharedConnection();
        }
    }

    // Transaction object helps maintain transaction depth counts
    public class Transaction : IDisposable
    {
        public Transaction(Database db)
        {
            _db = db;
            _db.BeginTransaction();
        }

        public virtual void Complete()
        {
            _db.CompleteTransaction();
            _db = null;
        }

        public void Dispose()
        {
            if (_db != null) 
                _db.AbortTransaction();
        }

        Database _db;
    }

    // Simple helper class for building SQL statments
    public class Sql
    {
        public Sql()
        {
        }

        public Sql(string sql, params object[] args)
        {
            _sql = sql;
            _args = args;
        }

        public static Sql Builder
        {
            get { return new Sql(); }
        }

        string _sql;
        object[] _args;
        Sql _rhs;
        string _sqlFinal;
        object[] _argsFinal;

        private void Build()
        {
            // already built?
            if (_sqlFinal != null) 
                return;

            // Build it
            var sb = new StringBuilder();
            var args = new List<object>();
            Build(sb, args, null);
            _sqlFinal = sb.ToString();
            _argsFinal = args.ToArray();
        }

        public string SQL
        {
            get
            {
                Build();
                return _sqlFinal;
            }
        }

        public object[] Arguments
        {
            get
            {
                Build();
                return _argsFinal;
            }
        }

        public Sql Append(Sql sql)
        {
            if (_rhs != null) 
                _rhs.Append(sql);
            else  
                _rhs = sql;
            return this;
        }

        public Sql Append(string sql, params object[] args)
        {
            return Append(new Sql(sql, args));
        }

        static bool Is(Sql sql, string sqltype)
        {
            return sql != null && sql._sql != null && sql._sql.StartsWith(sqltype, StringComparison.InvariantCultureIgnoreCase);
        }

        private void Build(StringBuilder sb, List<object> args, Sql lhs)
        {
            if (!String.IsNullOrEmpty(_sql))
            {
                // Add SQL to the string
                if (sb.Length > 0)
                {
                    sb.Append("\n");
                }

                var sql = Database.ProcessParams(_sql, _args, args);

                if (Is(lhs, "WHERE ") && Is(this, "WHERE ")) 
                    sql = "AND " + sql.Substring(6);
                if (Is(lhs, "ORDER BY ") && Is(this, "ORDER BY ")) 
                    sql = ", " + sql.Substring(9);

                sb.Append(sql);
            }

            // Now do rhs
            if (_rhs != null)
                _rhs.Build(sb, args, this);
        }

        public Sql Where(string sql, params object[] args)
        {
            return Append(new Sql("WHERE (" + sql + ")", args));
        }

        public Sql OrderBy(params object[] columns)
        {
            return Append(new Sql("ORDER BY " + String.Join(", ", (from x in columns select x.ToString()).ToArray())));
        }

        public Sql Select(params object[] columns)
        {
            return Append(new Sql("SELECT " + String.Join(", ", (from x in columns select x.ToString()).ToArray())));
        }

        public Sql From(params object[] tables)
        {
            return Append(new Sql("FROM " + String.Join(", ", (from x in tables select x.ToString()).ToArray())));
        }

        public Sql GroupBy(params object[] columns)
        {
            return Append(new Sql("GROUP BY " + String.Join(", ", (from x in columns select x.ToString()).ToArray())));
        }

        private SqlJoinClause Join(string JoinType, string table)
        {
            return new SqlJoinClause(Append(new Sql(JoinType + table)));
        }

        public SqlJoinClause InnerJoin(string table) { return Join("INNER JOIN ", table); }
        public SqlJoinClause LeftJoin(string table) { return Join("LEFT JOIN ", table); }

        public class SqlJoinClause
        {
            private readonly Sql _sql;

            public SqlJoinClause(Sql sql)
            {
                _sql = sql;
            }

            public Sql On(string onClause, params object[] args)
            {
                return _sql.Append("ON " + onClause, args);
            }
        }
    }
}
