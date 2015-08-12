using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Dynamic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using HA.Core.Toolkit;

namespace HA.Core
{
    public abstract class ConverterAttribute : Attribute
    {
        public abstract Func<object, object> GetConverter(Type srcType, Type dstType);
    }

    //public class XmlConverterAttribute : ConverterAttribute
    //{
    //    static MethodInfo fnGetList = typeof(TC.Finance.Toolkit.XmlHelper).GetMethod("GetEntityList", BindingFlags.Static | BindingFlags.Public);
    //    public override Func<object, object> GetConverter(Type srcType, Type dstType)
    //    {
    //        return (src) => (src == null ? null : fnGetList.MakeGenericMethod(dstType.GetGenericArguments()).Invoke(null, new object[] { src }));
    //    }
    //}

    //public class JsonConverterAttribute : ConverterAttribute
    //{
    //    public override Func<object, object> GetConverter(Type srcType, Type dstType)
    //    {
    //        return (src) => (src == null ? null : src.ToString().ToObject(dstType));
    //    }
    //}

    public class BaseDataConverterAttribute : ConverterAttribute
    {
        protected static Database _db = new Database("HA");

        public class BaseData
        {
            /// <summary>
            /// 编码
            /// </summary>
            [Column("BDCode")]
            public string Code { get; set; }

            /// <summary>
            /// 名称
            /// </summary>
            [Column("BDName")]
            public string Name { get; set; }
        }

        protected string _baseDataGroupCode;

        protected string _cacheKey;

        protected string _defaultValue;

        protected string _field;

        public BaseDataConverterAttribute(string baseDataGroupCode)
        {
            _baseDataGroupCode = baseDataGroupCode;
            _cacheKey = EncryptHelper.Md5Encrypt("PetaPoco.BaseDataConvertAttribute." + baseDataGroupCode);
        }

        public BaseDataConverterAttribute(string baseDataGroupCode, string defaultValue)
            : this(baseDataGroupCode)
        {
            _defaultValue = defaultValue;
        }

        public BaseDataConverterAttribute(string baseDataGroupCode, string defaultValue, string field)
            : this(baseDataGroupCode, defaultValue)
        {
            _field = field;
        }

        public override Func<object, object> GetConverter(Type srcType, Type dstType)
        {
            return src =>
            {
                if (src == null)
                {
                    return null;
                }
                if (string.IsNullOrWhiteSpace(src.ToString()))
                {
                    return string.Empty;
                }
                var baseDataList = GetBaseDataList();
                BaseData baseData;
                if (baseDataList.TryGetValue(src.ToString(), out baseData))
                {
                    //if (_field == "Remark")
                    //{
                    //    return baseData.Remark;
                    //}
                    return baseData.Name;
                }
                return _defaultValue;
            };
        }

        public Dictionary<string, BaseData> GetBaseDataList()
        {
            var baseDataList = CacheHelper.GetValue<Dictionary<string, BaseData>>(_cacheKey);
            if (baseDataList == null)
            {
                baseDataList = _db.Fetch<BaseData>(@"
SELECT
BD.BDCode,
BD.BDName
FROM dbo.FD_BaseData BD WITH(NOLOCK)
WHERE BD.RowStatus=0
AND BD.BDGroupCode=@0", _baseDataGroupCode).ToDictionary(t => t.Code);
                CacheHelper.Insert(_cacheKey, baseDataList, DateTime.UtcNow.AddHours(6));
            }
            return baseDataList;
        }
    }

    // marks property as a column and optionally supplies column name
    [AttributeUsage(AttributeTargets.Property)]
    public class ColumnAttribute : Attribute
    {
        public ColumnAttribute() { }
        public ColumnAttribute(string name) { Name = name; }
        public string Name { get; private set; }
    }

    // marks property as a result column and optionally supplies column name
    [AttributeUsage(AttributeTargets.Property)]
    public class ResultColumnAttribute : ColumnAttribute
    {
        public ResultColumnAttribute() { }
        public ResultColumnAttribute(string name) : base(name) { }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class ChildColumnAttribute : ResultColumnAttribute
    {
        public ChildColumnAttribute(string foreignKey) { ForeignKey = foreignKey; }
        public ChildColumnAttribute(string foreignKey, string name)
            : base(name)
        {
            ForeignKey = foreignKey;
        }
        public string ForeignKey { get; private set; }
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
        readonly string _connectionString;
        readonly DbProviderFactory _factory;
        IDbConnection _sharedConnection;
        IDbTransaction _transaction;
        int _sharedConnectionDepth;
        int _transactionDepth;
        bool _transactionCancelled;
        string _lastSql;
        object[] _lastArgs;

        public virtual void OnConnectionOpened(IDbConnection conn) { }
        public virtual void OnConnectionClosing(IDbConnection conn) { }
        public virtual void OnExecutingCommand(IDbCommand cmd) { }
        public virtual void OnExecutedCommand(IDbCommand cmd) { }
        public virtual void OnBeginTransaction() { }
        public virtual void OnEndTransaction() { }
        public virtual void OnException(Exception x)
        {
            throw new DataBaseException(x.Message, LastCommand);
        }

        public Database(string connectionStringName)
        {
            _connectionString = ConfigurationManager.ConnectionStrings[connectionStringName].ConnectionString;
            _factory = DbProviderFactories.GetFactory("System.Data.SqlClient");

            ForceDateTimesToUtc = true;
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
                // ReSharper disable once PossibleNullReferenceException
                _sharedConnection.ConnectionString = _connectionString;
                _sharedConnection.Open();

                OnConnectionOpened(_sharedConnection);

                if (KeepConnectionAlive)
                    _sharedConnectionDepth++;
            }
            _sharedConnectionDepth++;
        }

        // Close a previously opened connection
        public void CloseSharedConnection()
        {
            if (--_sharedConnectionDepth != 0)
                return;
            OnConnectionClosing(_sharedConnection);
            _sharedConnection.Dispose();
            _sharedConnection = null;
        }

        // Start a new transaction, can be nested, every call must be
        // matched by a call to AbortTransaction or CompleteTransaction
        // Use `using (var scope=db.Transaction) { scope.Complete(); }` to ensure correct semantics
        public void BeginTransaction()
        {
            if (++_transactionDepth != 1)
                return;
            OpenSharedConnection();
            _transaction = _sharedConnection.BeginTransaction();
            _transactionCancelled = false;
            OnBeginTransaction();
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
        static readonly Regex rxParams = new Regex(@"(?<!@)@\w+", RegexOptions.Compiled);
        public static string ProcessParams(string sql, object[] argsSrc, List<object> argsDest)
        {
            object argVal;
            return rxParams.Replace(sql, m =>
            {
                var param = m.Value.Substring(1);
                int paramIndex;
                if (!int.TryParse(param, out paramIndex))
                {
                    return m.Value;
                }
                // Numbered parameter
                if (paramIndex < 0 || paramIndex >= argsSrc.Length)
                    throw new ArgumentOutOfRangeException(string.Format("Parameter '@{0}' specified but only {1} parameters supplied (in `{2}`)", paramIndex, argsSrc.Length, sql));
                argVal = argsSrc[paramIndex];
                // Expand collections to parameter lists
                if (argVal is IEnumerable && !(argVal is string) && !(argVal is byte[]))
                {
                    var sb = new StringBuilder();
                    foreach (var val in argVal as IEnumerable)
                    {
                        sb.Append((sb.Length == 0 ? "@" : ",@") + argsDest.Count.ToString());
                        argsDest.Add(val);
                    }
                    return sb.ToString();
                }
                argsDest.Add(argVal);
                return "@" + (argsDest.Count - 1).ToString();
            });
        }

        // Add a parameter to a DB command
        static void AddParam(IDbCommand cmd, object item)
        {
            var p = cmd.CreateParameter();
            p.ParameterName = string.Format("@{0}", cmd.Parameters.Count);
            if (item == null)
            {
                p.Value = string.Empty;
            }
            else
            {
                var t = item.GetType();
                if (t.IsEnum)
                {
                    p.Value = (int)item;
                }
                else if (t == typeof(DateTime) && Convert.ToDateTime(item).Year == 1)
                {
                    p.Value = DateTime.Parse("1900-01-01");
                }
                else if (t == typeof(Guid))
                {
                    p.Value = item.ToString();
                    p.DbType = DbType.String;
                    p.Size = 40;
                }
                else if (t == typeof(string))
                {
                    // ReSharper disable once PossibleNullReferenceException
                    p.Size = Math.Max((item as string).Length, 4000);		// Help query plan caching by using common size
                    p.Value = item;
                }
                else if (t == typeof(AnsiString))
                {
                    // Thanks @DataChomp for pointing out the SQL Server indexing performance hit of using wrong string type on varchar
                    // ReSharper disable once PossibleNullReferenceException
                    p.Size = Math.Max((item as AnsiString).Value.Length, 4000);
                    // ReSharper disable once PossibleNullReferenceException
                    p.Value = (item as AnsiString).Value;
                    p.DbType = DbType.AnsiString;
                }
                else if (t == typeof(bool))
                {
                    p.Value = ((bool)item) ? 1 : 0;
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
            var newArgs = new List<object>();
            sql = ProcessParams(sql, args, newArgs);
            args = newArgs.ToArray();
            sql = sql.Replace("@@", "@");  // <- double @@ escapes a single @

            // Create the command and add parameters
            var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            cmd.Transaction = _transaction;

            foreach (var item in args)
            {
                AddParam(cmd, item);
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
                        var val = cmd.ExecuteScalar();
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

        static readonly Regex rxSelect = new Regex(@"\A\s*(SELECT|EXECUTE|CALL)\s", RegexOptions.Compiled | RegexOptions.Singleline | RegexOptions.IgnoreCase | RegexOptions.Multiline);
        static readonly Regex rxFrom = new Regex(@"\A\s*FROM\s", RegexOptions.Compiled | RegexOptions.Singleline | RegexOptions.IgnoreCase | RegexOptions.Multiline);

        public static string EscapeTableName(string str)
        {
            // Assume table names with "dot" are already escaped
            return str.IndexOf('.') >= 0 ? str : EscapeSqlIdentifier(str);
        }

        public static string EscapeSqlIdentifier(string str)
        {
            return string.Format("[{0}]", str);
        }

        public static bool IsNumericType(Type type)
        {
            var tc = Type.GetTypeCode(type);
            return tc >= TypeCode.SByte && tc <= TypeCode.Decimal;
        }

        public static string GetColumnName<T>(Expression<Func<T, object>> columnExpression)
        {
            var memberExpression = columnExpression.Body as MemberExpression;
            // ReSharper disable once ConvertIfStatementToNullCoalescingExpression
            if (memberExpression == null)
            {
                // ReSharper disable once PossibleNullReferenceException
                memberExpression = (columnExpression.Body as UnaryExpression).Operand as MemberExpression;
            }
            var pd = PocoData.ForType(typeof(T));
            // ReSharper disable once PossibleNullReferenceException
            return pd.Columns.FirstOrDefault(u => u.Value.PropertyInfo.Name == memberExpression.Member.Name).Value.ColumnName;
        }

        static string AddSelectClause<T>(string sql)
        {
            if (rxSelect.IsMatch(sql))
                return sql;
            var pd = PocoData.ForType(typeof(T));
            var tableName = EscapeTableName(pd.TableInfo.TableName);
            var cols = string.Join(", ", (from c in pd.QueryColumns select tableName + "." + EscapeSqlIdentifier(c)));
            // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
            if (!rxFrom.IsMatch(sql))
                sql = string.Format("SELECT {0} FROM {1} WITH(NOLOCK) {2}", cols, tableName, sql);
            else
                sql = string.Format("SELECT {0} {1}", cols, sql);
            return sql;
        }

        // Return an enumerable collection of pocos
        public IEnumerable<T> Query<T>(string sql, params object[] args)
        {
            sql = AddSelectClause<T>(sql);

            OpenSharedConnection();
            try
            {
                using (var cmd = CreateCommand(_sharedConnection, sql, args))
                {
                    IDataReader r;
                    var pd = PocoData.ForType(typeof(T));
                    try
                    {
                        r = cmd.ExecuteReader();
                        OnExecutedCommand(cmd);
                    }
                    catch (Exception x)
                    {
                        OnException(x);
                        throw;
                    }
                    var factory = pd.GetFactory(cmd.CommandText, _sharedConnection.ConnectionString, ForceDateTimesToUtc, 0, r.FieldCount, r) as Func<IDataReader, T>;
                    using (r)
                    {
                        while (true)
                        {
                            T poco;
                            try
                            {
                                if (!r.Read())
                                    yield break;
                                // ReSharper disable once PossibleNullReferenceException
                                poco = factory(r);
                            }
                            catch (Exception x)
                            {
                                OnException(x);
                                throw;
                            }
                            yield return poco;
                        }
                    }
                }
            }
            finally
            {
                CloseSharedConnection();
            }
        }

        public IEnumerable<T> Query<T>(Sql sql)
        {
            return Query<T>(sql.SQL, sql.Arguments);
        }

        // Return a typed list of pocos
        public List<T> Fetch<T>(string sql, params object[] args)
        {
            return Query<T>(sql, args).ToList();
        }

        public List<T> Fetch<T>(Sql sql)
        {
            return Fetch<T>(sql.SQL, sql.Arguments);
        }

        public List<T> Fetch<T>(Sql<T> sql)
        {
            return Fetch<T>(sql.SQL, sql.Arguments);
        }

        static readonly Regex rxColumns = new Regex(@"\A\s*SELECT\s+((?:\((?>\((?<depth>)|\)(?<-depth>)|.?)*(?(depth)(?!))\)|.)*?)(?<!,\s+)\bFROM\b", RegexOptions.IgnoreCase | RegexOptions.Multiline | RegexOptions.Singleline | RegexOptions.Compiled);
        static readonly Regex rxOrderBy = new Regex(@"\bORDER\s+BY\s+(?:\((?>\((?<depth>)|\)(?<-depth>)|.?)*(?(depth)(?!))\)|[\w\(\)\[\]\.])+(?:\s+(?:ASC|DESC))?(?:\s*,\s*(?:\((?>\((?<depth>)|\)(?<-depth>)|.?)*(?(depth)(?!))\)|[\w\(\)\[\]\.])+(?:\s+(?:ASC|DESC))?)*\s*\z", RegexOptions.IgnoreCase | RegexOptions.Multiline | RegexOptions.Singleline | RegexOptions.Compiled);
        static readonly Regex rxWhere = new Regex(@"\bWHERE\b\s+(.*?)\s*\z", RegexOptions.RightToLeft | RegexOptions.IgnoreCase | RegexOptions.Multiline | RegexOptions.Singleline | RegexOptions.Compiled);
        static readonly Regex rxTable = new Regex(@"\A\s*SELECT\s+((?:\((?>\((?<depth>)|\)(?<-depth>)|.?)*(?(depth)(?!))\)|.)*?)(?<!,\s+)(\bFROM\b\s+.*?)\s+\n", RegexOptions.IgnoreCase | RegexOptions.Multiline | RegexOptions.Singleline | RegexOptions.Compiled);

        public static bool SplitSqlForPaging(string sql, out string sqlCount, out string sqlSelectRemoved, out string sqlOrderBy, bool isEase)
        {
            sqlSelectRemoved = null;
            sqlCount = null;
            sqlOrderBy = null;

            // Extract the columns from "SELECT <whatever> FROM"
            var m = rxColumns.Match(sql);
            if (!m.Success)
                return false;

            // Save column list and replace with COUNT(*)
            var g = m.Groups[1];
            sqlSelectRemoved = sql.Substring(g.Index);

            if (isEase)
            {
                m = rxTable.Match(sql);
                var gFrom = m.Groups[2];
                var mWhere = rxWhere.Match(sql);
                var gWhere = mWhere.Groups[0];
                sqlCount = sql.Substring(0, g.Index) + "COUNT(*) " + sql.Substring(gFrom.Index, gFrom.Length) + " " + sql.Substring(gWhere.Index, gWhere.Length);
            }
            else
            {
                sqlCount = sql.Substring(0, g.Index) + "COUNT(*) " + sql.Substring(g.Index + g.Length);
            }


            // Look for an "ORDER BY <whatever>" clause
            m = rxOrderBy.Match(sqlCount);
            if (!m.Success)
                return true;

            g = m.Groups[0];
            sqlOrderBy = g.ToString();
            sqlCount = sqlCount.Substring(0, g.Index) + sqlCount.Substring(g.Index + g.Length);

            return true;
        }

        public void BuildPageQueries<T>(long skip, long take, string sql, ref object[] args, out string sqlCount, out string sqlPage, bool isEase = false)
        {
            // Add auto select clause
            sql = AddSelectClause<T>(sql);

            // Split the SQL into the bits we need
            string sqlSelectRemoved, sqlOrderBy;
            if (!SplitSqlForPaging(sql, out sqlCount, out sqlSelectRemoved, out sqlOrderBy, isEase))
                throw new Exception("Unable to parse SQL statement for paged query");

            sqlSelectRemoved = rxOrderBy.Replace(sqlSelectRemoved, "");

            sqlPage = string.Format("SELECT * FROM (SELECT ROW_NUMBER() OVER ({0}) peta_rn, {1}) peta_paged WHERE peta_rn>@{2} AND peta_rn<=@{3}",
                                    sqlOrderBy ?? "ORDER BY (SELECT NULL)", sqlSelectRemoved, args.Length, args.Length + 1);
            args = args.Concat(new object[] { skip, skip + take }).ToArray();
        }

        public Page<T> Page<T>(long page, long itemsPerPage, string sql, bool isEase , params object[] args)
        {
            string sqlCount, sqlPage;
            BuildPageQueries<T>((page - 1) * itemsPerPage, itemsPerPage, sql, ref args, out sqlCount, out sqlPage, isEase);

            // Save the one-time command time out and use it for both queries
            var saveTimeout = OneTimeCommandTimeout;

            // Setup the paged result
            var result = new Page<T>
            {
                CurrentPage = page,
                ItemsPerPage = itemsPerPage,
                TotalItems = ExecuteScalar<long>(sqlCount, args)
            };
            result.TotalPages = result.TotalItems / itemsPerPage;
            if ((result.TotalItems % itemsPerPage) != 0)
                result.TotalPages++;

            OneTimeCommandTimeout = saveTimeout;

            // Get the records
            result.Items = Fetch<T>(sqlPage, args);

            // Done
            return result;
        }

        public Page<T> Page<T>(long page, long itemsPerPage, string sql, params object[] args)
        {
            return Page<T>(page, itemsPerPage, sql, false, args);
        }

        public Page<T> Page<T>(long page, long itemsPerPage, Sql sql)
        {
            return Page<T>(page, itemsPerPage, sql.SQL, sql.Arguments);
        }

        public Page<T> EasePage<T>(long page, long itemsPerPage, string sql, params object[] args)
        {
            return Page<T>(page, itemsPerPage, sql, true, args);
        }

        public Page<T> EasePage<T>(long page, long itemsPerPage, Sql sql)
        {
            return Page<T>(page, itemsPerPage, sql.SQL, true, sql.Arguments);
        }

        public List<T> SkipTake<T>(long skip, long take, string sql, params object[] args)
        {
            string sqlCount, sqlPage;
            BuildPageQueries<T>(skip, take, sql, ref args, out sqlCount, out sqlPage);
            return Fetch<T>(sqlPage, args);
        }

        public List<T> SkipTake<T>(long skip, long take, Sql sql)
        {
            return SkipTake<T>(skip, take, sql.SQL, sql.Arguments);
        }

        public object Insert(object poco)
        {
            try
            {
                OpenSharedConnection();
                try
                {
                    using (var cmd = CreateCommand(_sharedConnection, ""))
                    {
                        var pd = PocoData.ForType(poco.GetType());
                        var tableName = pd.TableInfo.TableName;
                        var autoIncrement = pd.TableInfo.AutoIncrement;
                        var primaryKeyName = pd.TableInfo.PrimaryKey;
                        var columns= pd.Columns.Values.Where(c => !c.ResultColumn && !c.AutoIncrement);
                        var names = new List<string>();
                        var values = new List<string>();
                        var index = 0;
                        foreach (var c in columns)
                        {
                            names.Add(EscapeSqlIdentifier(c.ColumnName));
                            values.Add(string.Format("@{0}", index++));
                            AddParam(cmd, c.GetValue(poco));
                        }

                        cmd.CommandText = string.Format("INSERT INTO {0} ({1}) VALUES ({2})",
                                EscapeTableName(tableName),
                                string.Join(",", names),
                                string.Join(",", values)
                                );

                        if (!autoIncrement)
                        {
                            DoPreExecute(cmd);
                            cmd.ExecuteNonQuery();
                            OnExecutedCommand(cmd);
                            return true;
                        }

                        cmd.CommandText += ";\nSELECT SCOPE_IDENTITY() AS NewID;";
                        DoPreExecute(cmd);
                        var id = cmd.ExecuteScalar();
                        OnExecutedCommand(cmd);

                        // Assign the ID back to the primary key property
                        if (primaryKeyName != null)
                        {
                            PocoColumn pc;
                            if (pd.Columns.TryGetValue(primaryKeyName, out pc))
                            {
                                pc.SetValue(poco, pc.ChangeType(id));
                            }
                        }
                        return id;
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

        private static DataTable CopyToDataTable<T>(IEnumerable<T> collection, PocoColumn[] columns)
        {
            var dt = new DataTable();
            foreach (var column in columns)
            {
                dt.Columns.Add(column.PropertyInfo.Name);
            }
            foreach (var poco in collection)
            {
                var values = new object[columns.Length];
                for (var i = 0; i < columns.Length; i++)
                {
                    values[i] = columns[i].GetValue(poco);
                    if (values[i] == null)
                    {
                        values[i] = string.Empty;
                    }
                    else if (values[i] is DateTime && Convert.ToDateTime(values[i]).Year == 1)
                    {
                        values[i] = DateTime.Parse("1900-01-01");
                    }
                }
                dt.Rows.Add(values);
            }
            return dt;
        }

        public void BulkCopy<T>(IEnumerable<T> collection)
        {
            var pd = PocoData.ForType(typeof(T));
            var columns = pd.Columns.Values.Where(c => !c.ResultColumn && !c.AutoIncrement).ToArray();
            var dt = CopyToDataTable(collection, columns);
            try
            {
                OpenSharedConnection();
                try
                {
                    using (var bulkCopy = new SqlBulkCopy((SqlConnection)_sharedConnection, SqlBulkCopyOptions.Default, (SqlTransaction)_transaction))
                    {
                        bulkCopy.DestinationTableName = pd.TableInfo.TableName;
                        foreach (var column in columns)
                        {
                            bulkCopy.ColumnMappings.Add(column.PropertyInfo.Name, column.ColumnName);
                        }
                        bulkCopy.WriteToServer(dt);
                        bulkCopy.Close();
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

        private int BulkInsertProcess<T>(IEnumerable<T> collection, Func<string, T, string> sqlRebuild)
        {
            using (var cmd = CreateCommand(_sharedConnection, ""))
            {
                var sql = new StringBuilder();
                sql.AppendLine("BEGIN TRY");
                // insert sql
                var pd = PocoData.ForType(typeof(T));
                var tableName = EscapeTableName(pd.TableInfo.TableName);
                var columns = pd.Columns.Values.Where(c => !c.ResultColumn && !c.AutoIncrement).ToList();

                var childColumns = pd.Columns.Values.Where(c => c.ChildColumn).ToList();
                if(childColumns.Count>0)
                {
                    sql.AppendLine("DECLARE @Id NUMERIC");
                }
                var childItemColumn = new Dictionary<string, List<PocoColumn>>();
                var childItemColsStr = new Dictionary<string, string>();
                var childItemTableName = new Dictionary<string, string>();
                childColumns.ForEach(t =>
                {
                    var type = t.PropertyInfo.PropertyType;
                    var pdChildItem = type.IsGenericType ? PocoData.ForType(type.GenericTypeArguments[0]) : PocoData.ForType(type);
                    childItemTableName.Add(t.ColumnName, EscapeTableName(pdChildItem.TableInfo.TableName));
                    var childItemColumns = pdChildItem.Columns.Values.Where(c => !c.ResultColumn && !c.AutoIncrement && c.ColumnName != t.ForeignKey).ToList();
                    childItemColumn.Add(t.ColumnName, childItemColumns);
                    childItemColsStr.Add(t.ColumnName, EscapeSqlIdentifier(t.ForeignKey) + "," + string.Join(",", childItemColumns.Select(c => EscapeSqlIdentifier(c.ColumnName))));
                });

                var colsStr = string.Join(",", columns.Select(c => EscapeSqlIdentifier(c.ColumnName)));
                foreach (var poco in collection)
                {
                    var values = GetInsertValueStringList(columns, poco);
                    var insertSql = string.Format(@"INSERT {0} ({1}) VALUES ({2});", tableName, colsStr, string.Join(",", values));
                    if (childColumns.Count > 0)
                    {
                        var sqlAppend = new StringBuilder();
                        foreach (var c in childColumns)
                        {
                            var value = c.GetValue(poco);
                            if (value == null)
                            {
                                continue;
                            }
                            var childItems = value as IList;
                            if (childItems != null && childItems.Count == 0)
                            {
                                continue;
                            }
                            childItems = childItems ?? new[] { value };
                            BuildChildBulkInsertSql(childItemTableName[c.ColumnName], childItemColumn[c.ColumnName], childItemColsStr[c.ColumnName], childItems, sqlAppend);
                        }
                        if (sqlAppend.Length > 0)
                        {
                            insertSql = insertSql + "SET @Id=SCOPE_IDENTITY();" + sqlAppend;
                        }
                    }
                    if (sqlRebuild != null)
                    {
                        insertSql = sqlRebuild(insertSql, poco);
                    }
                    sql.AppendLine(insertSql);
                }
                sql.AppendLine(@"
END TRY
BEGIN CATCH
    DECLARE @ErrorMessage NVARCHAR(4000)=ERROR_MESSAGE();
    DECLARE @ErrorSeverity INT=ERROR_SEVERITY();
    DECLARE @ErrorState INT=ERROR_STATE();
    RAISERROR (@ErrorMessage, @ErrorSeverity,@ErrorState);
END CATCH
");
                cmd.CommandText = sql.ToString();
                DoPreExecute(cmd);
                var returnValue = cmd.ExecuteNonQuery();
                OnExecutedCommand(cmd);
                return returnValue;
            }
        }

        private static void BuildChildBulkInsertSql(string tableName, IEnumerable<PocoColumn> columns, string colsStr, IEnumerable collection, StringBuilder sql)
        {
            var values = (from object poco in collection select "(@Id," + string.Join(",", GetInsertValueStringList(columns, poco)) + ")");
            sql.AppendFormat("INSERT {0} ({1}) VALUES {2};", tableName, colsStr, string.Join(",", values));
        }

        private static IEnumerable<string> GetInsertValueStringList(IEnumerable<PocoColumn> columns, object poco)
        {
            return columns.Select(t => GetInsertValueString(t, poco));
        }

        private static string GetInsertValueString(PocoColumn column, object poco)
        {
            var value = column.GetValue(poco);
            if (value == null)
            {
                return "''";
            }
            if (IsNumericType(column.PropertyInfo.PropertyType))
            {
                return value.ToString();
            }
            var type = value.GetType();
            if (type.IsEnum)
            {
                value = (int)value;
                return value.ToString();
            }
            if (type == typeof(DateTime))
            {
                value = Convert.ToDateTime(value).Year == 1 ? "1900-01-01" : Convert.ToDateTime(value).ToString("yyyy-MM-dd HH:mm:ss.fff");
            }
            return "N'" + value + "'";
        }

        public int BulkInsert<T>(IList<T> collection, Func<string, T, string> sqlRebuild = null)
        {
            try
            {
                var returnValue = 0;
                var rowIndex = 0;
                var rowCount = collection.Count();
                using (var scope = GetTransaction())
                {
                    while (rowIndex < rowCount)
                    {
                        returnValue += BulkInsertProcess(collection.Skip(rowIndex).Take(1000).ToList(), sqlRebuild);
                        rowIndex += 1000;
                    }
                    scope.Complete();
                }
                return returnValue;
            }
            catch (Exception x)
            {
                OnException(x);
                throw;
            }
        }

        private int BulkInsertProcess<T>(IEnumerable<T> collection, string whereSql)
        {
            OpenSharedConnection();
            using (var cmd = CreateCommand(_sharedConnection, ""))
            {
                var pd = PocoData.ForType(typeof(T));
                var columns = pd.Columns.Values.Where(c => !c.ResultColumn && !c.AutoIncrement).ToList();
                var colsStr = string.Join(", ", columns.Select(c => EscapeSqlIdentifier(c.ColumnName)));
                var sql = new StringBuilder();
                sql.AppendLine("BEGIN TRY");
                sql.AppendLine("IF(OBJECT_ID('tempdb..#T1') IS NOT NULL) DROP TABLE #T1");
                sql.AppendFormat("SELECT {0} INTO #T1 FROM {1} WHERE 1=2", colsStr, EscapeTableName(pd.TableInfo.TableName)).AppendLine();
                foreach (var poco in collection)
                {
                    var values = GetInsertValueStringList(columns, poco);
                    sql.AppendFormat(@"INSERT #T1 VALUES ({0});", string.Join(",", values)).AppendLine();
                }
                sql.AppendFormat(@"INSERT {0} ({1}) SELECT {1} FROM #T1", EscapeTableName(pd.TableInfo.TableName), colsStr).AppendLine();
                if (!string.IsNullOrEmpty(whereSql))
                {
                    sql.AppendLine(whereSql);
                }
                sql.AppendLine(@"
END TRY
BEGIN CATCH
    DECLARE @ErrorMessage NVARCHAR(4000)=ERROR_MESSAGE();
    DECLARE @ErrorSeverity INT=ERROR_SEVERITY();
    DECLARE @ErrorState INT=ERROR_STATE();
    RAISERROR (@ErrorMessage, @ErrorSeverity,@ErrorState);
END CATCH
");
                cmd.CommandText = sql.ToString();
                DoPreExecute(cmd);
                var returnValue = cmd.ExecuteNonQuery();
                OnExecutedCommand(cmd);
                return returnValue;
            }
        }

        public int BulkInsert<T>(IList<T> collection, string whereSql)
        {
            try
            {
                var returnValue = 0;
                var rowIndex = 0;
                var rowCount = collection.Count();
                using (var scope = GetTransaction())
                {
                    while (rowIndex < rowCount)
                    {
                        returnValue += BulkInsertProcess(collection.Skip(rowIndex).Take(1000).ToList(), whereSql);
                        rowIndex += 1000;
                    }
                    scope.Complete();
                }
                return returnValue;
            }
            catch (Exception x)
            {
                OnException(x);
                throw;
            }
        }

        // Update a record with values from a poco.  primary key value can be either supplied or read from the poco
        private int Update(object poco, string[] columns)
        {
            try
            {
                OpenSharedConnection();
                try
                {
                    using (var cmd = CreateCommand(_sharedConnection, ""))
                    {
                        var pd = PocoData.ForType(poco.GetType());
                        var tableName = pd.TableInfo.TableName;
                        var primaryKeyName = pd.TableInfo.PrimaryKey;
                        var sb = new StringBuilder();
                        var index = 0;
                        columns = columns.Length > 0 ? columns : pd.Columns.Values.Where(c => !c.ResultColumn && !c.AutoIncrement).Select(c => c.ColumnName).ToArray();
                        foreach (var colname in columns)
                        {
                            var pc = pd.Columns[colname];

                            // Build the sql
                            if (index > 0)
                                sb.Append(", ");
                            sb.AppendFormat("{0} = @{1}", EscapeSqlIdentifier(colname), index++);

                            // Store the parameter in the command
                            AddParam(cmd, pc.GetValue(poco));
                        }
                        var primaryKeyValue = pd.Columns[primaryKeyName].GetValue(poco);

                        cmd.CommandText = string.Format("UPDATE {0} SET {1} WHERE {2} = @{3}",
                            EscapeTableName(tableName), sb, EscapeSqlIdentifier(primaryKeyName), index);
                        AddParam(cmd, primaryKeyValue);

                        DoPreExecute(cmd);

                        // Do it
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

        public int Update<T>(T poco, params Expression<Func<T, object>>[] expressions)
        {
            return Update(poco, (from c in expressions select GetColumnName(c)).ToArray());
        }

        private int BulkUpdateProcess<T>(IEnumerable<T> collection,string[] primaryKeyNames, string[] columnNames, Func<string, T, string> sqlRebuild)
        {
            using (var cmd = CreateCommand(_sharedConnection, ""))
            {
                var pd = PocoData.ForType(typeof(T));
                var columns = pd.Columns.Values.Where(c => columnNames.Contains(c.ColumnName)).ToList();
                var primaryKeyColumns = pd.Columns.Values.Where(c => primaryKeyNames.Contains(c.ColumnName)).ToList();
                var sql = new StringBuilder();
                sql.AppendLine("BEGIN TRY");
                foreach (var poco in collection)
                {
                    var obj = poco;
                    var setSqls = columns.Select(t => string.Format("{0}={1}", EscapeSqlIdentifier(t.ColumnName), GetInsertValueString(t, obj))).ToList();
                    var whereSqls = primaryKeyColumns.Select(t => string.Format("{0}={1}", EscapeSqlIdentifier(t.ColumnName), GetInsertValueString(t, obj))).ToList();
                    var updateSql = string.Format(@"UPDATE {0} SET {1} WHERE {2};", EscapeSqlIdentifier(pd.TableInfo.TableName), string.Join(",", setSqls), string.Join(" AND ", whereSqls));
                    if (sqlRebuild != null)
                    {
                        updateSql = sqlRebuild(updateSql, obj);
                    }
                    sql.AppendLine(updateSql);
                }
                sql.AppendFormat(@"
END TRY
BEGIN CATCH
    DECLARE @ErrorMessage NVARCHAR(4000)=ERROR_MESSAGE();
    DECLARE @ErrorSeverity INT=ERROR_SEVERITY();
    DECLARE @ErrorState INT=ERROR_STATE();
    RAISERROR (@ErrorMessage, @ErrorSeverity,@ErrorState);
END CATCH
");
                cmd.CommandText = sql.ToString();
                DoPreExecute(cmd);
                var returnValue = cmd.ExecuteNonQuery();
                OnExecutedCommand(cmd);
                return returnValue;
            }
        }

        public int BulkUpdate<T>(IList<T> collection, string[] primaryKeyNames, string[] columnNames, Func<string, T, string> sqlRebuild)
        {
            try
            {
                var returnValue = 0;
                var rowIndex = 0;
                var rowCount = collection.Count();
                using (var scope = GetTransaction())
                {
                    while (rowIndex < rowCount)
                    {
                        returnValue += BulkUpdateProcess(collection.Skip(rowIndex).Take(1000).ToList(), primaryKeyNames, columnNames, sqlRebuild);
                        rowIndex += 1000;
                    }
                    scope.Complete();
                }
                return returnValue;
            }
            catch (Exception x)
            {
                OnException(x);
                throw;
            }
        }

        public int BulkUpdate<T>(IList<T> collection, params Expression<Func<T, object>>[] expressions)
        {
            var pd = PocoData.ForType(typeof(T));
            return BulkUpdate(collection, new[] { pd.TableInfo.PrimaryKey }, (from c in expressions select GetColumnName(c)).ToArray(), null);
        }

        public int BulkUpdate<T>(IList<T> collection,Expression<Func<T, object>>[] primaryKeyExpressions, params Expression<Func<T, object>>[] expressions)
        {
            return BulkUpdate(collection, primaryKeyExpressions, null, expressions);
        }

        public int BulkUpdate<T>(IList<T> collection, Expression<Func<T, object>>[] primaryKeyExpressions,Func<string, T, string> sqlRebuild, params Expression<Func<T, object>>[] expressions)
        {
            return BulkUpdate(collection, (from c in primaryKeyExpressions select GetColumnName(c)).ToArray(), (from c in expressions select GetColumnName(c)).ToArray(), sqlRebuild);
        }

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

        public string LastSQL { get { return _lastSql; } }
        public object[] LastArgs { get { return _lastArgs; } }

        public string LastCommand
        {
            get { return FormatCommand(_lastSql, _lastArgs); }
        }

        public string FormatCommand(IDbCommand cmd)
        {
            return FormatCommand(cmd.CommandText, (from IDataParameter parameter in cmd.Parameters select parameter.Value).ToArray());
        }

        public string FormatCommand(string sql, object[] args)
        {
            var sb = new StringBuilder();
            if (sql == null)
                return "";
            sb.Append(sql);
            if (args != null && args.Length > 0)
            {
                sb.Append("\n");
                for (var i = 0; i < args.Length; i++)
                {
                    sb.AppendFormat("\t -> @{0} [{1}] = \"{2}\"\n", i, args[i].GetType().Name, args[i]);
                }
                sb.Remove(sb.Length - 1, 1);
            }
            return sb.ToString();
        }

        // Automatically close one open shared connection
        public void Dispose()
        {
            // Automatically close one open connection reference
            //  (Works with KeepConnectionAlive and manually opening a shared connection)
            CloseSharedConnection();
        }

        public class PocoColumn
        {
            public string ColumnName;
            public PropertyInfo PropertyInfo;
            public bool ResultColumn;
            public bool ChildColumn;
            public string ForeignKey;
            public bool AutoIncrement;
            public virtual void SetValue(object target, object val) { PropertyInfo.SetValue(target, val, null); }
            public virtual object GetValue(object target) { return PropertyInfo.GetValue(target, null); }
            public virtual object ChangeType(object val) { return Convert.ChangeType(val, PropertyInfo.PropertyType); }
        }

        public class PocoData
        {
            public PocoData()
            {
            }

            static readonly ReaderWriterLockSlim RwLock = new ReaderWriterLockSlim();
            public static PocoData ForType(Type t)
            {
                // Check cache
                RwLock.EnterReadLock();
                PocoData pd;
                try
                {
                    if (m_PocoDatas.TryGetValue(t, out pd))
                        return pd;
                }
                finally
                {
                    RwLock.ExitReadLock();
                }

                // Cache it
                RwLock.EnterWriteLock();
                try
                {
                    // Check again
                    if (m_PocoDatas.TryGetValue(t, out pd))
                        return pd;

                    // Create it
                    pd = new PocoData(t);

                    m_PocoDatas.Add(t, pd);
                }
                finally
                {
                    RwLock.ExitWriteLock();
                }
                return pd;
            }

            public PocoData(Type t)
            {
                Type = t;
                TableInfo = new TableInfo();

                // Get the table name
                var tableNameAtt = t.GetCustomAttributes<TableNameAttribute>(true).FirstOrDefault();
                TableInfo.TableName = tableNameAtt == null ? t.Name : tableNameAtt.Value;

                // Get the primary key
                var primaryKeyAttr = t.GetCustomAttributes<PrimaryKeyAttribute>(true).FirstOrDefault();
                TableInfo.PrimaryKey = primaryKeyAttr == null ? "ID" : primaryKeyAttr.Value;
                TableInfo.AutoIncrement = primaryKeyAttr != null && primaryKeyAttr.AutoIncrement;

                Columns = new Dictionary<string, PocoColumn>(StringComparer.OrdinalIgnoreCase);
                foreach (var pi in t.GetProperties())
                {
                    // Work out if properties is to be included
                    var colAttr = pi.GetCustomAttributes<ColumnAttribute>(true).FirstOrDefault();
                    if (colAttr == null)
                        continue;

                    var pc = new PocoColumn
                    {
                        PropertyInfo = pi,
                        ColumnName = string.IsNullOrWhiteSpace(colAttr.Name) ? pi.Name : colAttr.Name
                    };

                    // Work out the DB column name
                    if ((colAttr as ResultColumnAttribute) != null)
                        pc.ResultColumn = true;
                    if ((colAttr as ChildColumnAttribute) != null)
                    {
                        pc.ChildColumn = true;
                        pc.ForeignKey = (colAttr as ChildColumnAttribute).ForeignKey;
                    }
                    if (TableInfo.AutoIncrement && string.Compare(TableInfo.PrimaryKey, pc.ColumnName, StringComparison.OrdinalIgnoreCase) == 0)
                        pc.AutoIncrement = true;

                    // Store it
                    Columns.Add(pc.ColumnName, pc);
                }

                // Build column list for automatic select
                QueryColumns = (from c in Columns where !c.Value.ResultColumn select c.Key).ToArray();
            }

            static bool IsIntegralType(Type t)
            {
                var tc = Type.GetTypeCode(t);
                return tc >= TypeCode.SByte && tc <= TypeCode.UInt64;
            }

            // Create factory function that can convert a IDataReader record into a POCO
            public Delegate GetFactory(string sql, string connString, bool forceDateTimesToUtc, int firstColumn, int countColumns, IDataReader r)
            {
                // Check cache
                var key = string.Format("{0}:{1}:{2}:{3}:{4}", sql, connString, forceDateTimesToUtc, firstColumn, countColumns);
                RwLock.EnterReadLock();
                try
                {
                    // Have we already created it?
                    Delegate factory;
                    if (_pocoFactories.TryGetValue(key, out factory))
                        return factory;
                }
                finally
                {
                    RwLock.ExitReadLock();
                }

                // Take the writer lock
                RwLock.EnterWriteLock();

                try
                {
                    // Check again, just in case
                    Delegate factory;
                    if (_pocoFactories.TryGetValue(key, out factory))
                        return factory;

                    // Create the method
                    var m = new DynamicMethod("petapoco_factory_" + _pocoFactories.Count, Type, new[] { typeof(IDataReader) }, true);
                    var il = m.GetILGenerator();

                    if (Type == typeof(object))
                    {
                        // var poco=new T()
                        // ReSharper disable once AssignNullToNotNullAttribute
                        il.Emit(OpCodes.Newobj, typeof(ExpandoObject).GetConstructor(Type.EmptyTypes));			// obj

                        var fnAdd = typeof(IDictionary<string, object>).GetMethod("Add");

                        // Enumerate all fields generating a set assignment for the column
                        for (var i = firstColumn; i < firstColumn + countColumns; i++)
                        {
                            var srcType = r.GetFieldType(i);

                            il.Emit(OpCodes.Dup);						// obj, obj
                            il.Emit(OpCodes.Ldstr, r.GetName(i));		// obj, obj, fieldname

                            // Get the converter
                            Func<object, object> converter = null;

                            if (forceDateTimesToUtc && srcType == typeof(DateTime))
                                converter = src => new DateTime(((DateTime)src).Ticks, DateTimeKind.Utc);

                            // Setup stack for call to converter
                            AddConverterToStack(il, converter);

                            // r[i]
                            il.Emit(OpCodes.Ldarg_0);					// obj, obj, fieldname, converter?,    rdr
                            il.Emit(OpCodes.Ldc_I4, i);					// obj, obj, fieldname, converter?,  rdr,i
                            il.Emit(OpCodes.Callvirt, fnGetValue);		// obj, obj, fieldname, converter?,  value

                            // Convert DBNull to null
                            il.Emit(OpCodes.Dup);						// obj, obj, fieldname, converter?,  value, value
                            il.Emit(OpCodes.Isinst, typeof(DBNull));	// obj, obj, fieldname, converter?,  value, (value or null)
                            var lblNotNull = il.DefineLabel();
                            il.Emit(OpCodes.Brfalse_S, lblNotNull);		// obj, obj, fieldname, converter?,  value
                            il.Emit(OpCodes.Pop);						// obj, obj, fieldname, converter?
                            if (converter != null)
                                il.Emit(OpCodes.Pop);					// obj, obj, fieldname, 
                            il.Emit(OpCodes.Ldnull);					// obj, obj, fieldname, null
                            if (converter != null)
                            {
                                var lblReady = il.DefineLabel();
                                il.Emit(OpCodes.Br_S, lblReady);
                                il.MarkLabel(lblNotNull);
                                il.Emit(OpCodes.Callvirt, fnInvoke);
                                il.MarkLabel(lblReady);
                            }
                            else
                            {
                                il.MarkLabel(lblNotNull);
                            }

                            il.Emit(OpCodes.Callvirt, fnAdd);
                        }
                    }
                    else if (Type.IsValueType || Type == typeof(string) || Type == typeof(byte[]))
                    {
                        // Do we need to install a converter?
                        var srcType = r.GetFieldType(0);
                        var converter = GetConverter(forceDateTimesToUtc, null, srcType, Type);

                        // "if (!rdr.IsDBNull(i))"
                        il.Emit(OpCodes.Ldarg_0);										// rdr
                        il.Emit(OpCodes.Ldc_I4_0);										// rdr,0
                        il.Emit(OpCodes.Callvirt, fnIsDBNull);							// bool
                        var lblCont = il.DefineLabel();
                        il.Emit(OpCodes.Brfalse_S, lblCont);
                        il.Emit(OpCodes.Ldnull);										// null
                        var lblFin = il.DefineLabel();
                        il.Emit(OpCodes.Br_S, lblFin);

                        il.MarkLabel(lblCont);

                        // Setup stack for call to converter
                        AddConverterToStack(il, converter);

                        il.Emit(OpCodes.Ldarg_0);										// rdr
                        il.Emit(OpCodes.Ldc_I4_0);										// rdr,0
                        il.Emit(OpCodes.Callvirt, fnGetValue);							// value

                        // Call the converter
                        if (converter != null)
                            il.Emit(OpCodes.Callvirt, fnInvoke);

                        il.MarkLabel(lblFin);
                        il.Emit(OpCodes.Unbox_Any, Type);								// value converted
                    }
                    else
                    {
                        // var poco=new T()
                        il.Emit(OpCodes.Newobj, Type.GetConstructor(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic, null, new Type[0], null));

                        // Enumerate all fields generating a set assignment for the column
                        for (var i = firstColumn; i < firstColumn + countColumns; i++)
                        {
                            // Get the PocoColumn for this db column, ignore if not known
                            PocoColumn pc;
                            if (!Columns.TryGetValue(r.GetName(i), out pc))
                                continue;

                            // Get the source type for this column
                            var srcType = r.GetFieldType(i);
                            var dstType = pc.PropertyInfo.PropertyType;

                            // "if (!rdr.IsDBNull(i))"
                            il.Emit(OpCodes.Ldarg_0);										// poco,rdr
                            il.Emit(OpCodes.Ldc_I4, i);										// poco,rdr,i
                            il.Emit(OpCodes.Callvirt, fnIsDBNull);							// poco,bool
                            var lblNext = il.DefineLabel();
                            il.Emit(OpCodes.Brtrue_S, lblNext);								// poco

                            il.Emit(OpCodes.Dup);											// poco,poco

                            // Do we need to install a converter?
                            var converter = GetConverter(forceDateTimesToUtc, pc, srcType, dstType);

                            // Fast
                            var handled = false;
                            if (converter == null)
                            {
                                var valuegetter = typeof(IDataRecord).GetMethod("Get" + srcType.Name, new[] { typeof(int) });
                                if (valuegetter != null
                                        && valuegetter.ReturnType == srcType
                                        && (valuegetter.ReturnType == dstType || valuegetter.ReturnType == Nullable.GetUnderlyingType(dstType)))
                                {
                                    il.Emit(OpCodes.Ldarg_0);										// *,rdr
                                    il.Emit(OpCodes.Ldc_I4, i);										// *,rdr,i
                                    il.Emit(OpCodes.Callvirt, valuegetter);							// *,value

                                    // Convert to Nullable
                                    if (Nullable.GetUnderlyingType(dstType) != null)
                                    {
                                        // ReSharper disable once AssignNullToNotNullAttribute
                                        il.Emit(OpCodes.Newobj, dstType.GetConstructor(new[] { Nullable.GetUnderlyingType(dstType) }));
                                    }

                                    il.Emit(OpCodes.Callvirt, pc.PropertyInfo.GetSetMethod(true));		// poco
                                    handled = true;
                                }
                            }

                            // Not so fast
                            if (!handled)
                            {
                                // Setup stack for call to converter
                                AddConverterToStack(il, converter);

                                // "value = rdr.GetValue(i)"
                                il.Emit(OpCodes.Ldarg_0);										// *,rdr
                                il.Emit(OpCodes.Ldc_I4, i);										// *,rdr,i
                                il.Emit(OpCodes.Callvirt, fnGetValue);							// *,value

                                // Call the converter
                                if (converter != null)
                                    il.Emit(OpCodes.Callvirt, fnInvoke);

                                // Assign it
                                il.Emit(OpCodes.Unbox_Any, pc.PropertyInfo.PropertyType);		// poco,poco,value
                                il.Emit(OpCodes.Callvirt, pc.PropertyInfo.GetSetMethod(true));		// poco
                            }

                            il.MarkLabel(lblNext);
                        }

                        var fnOnLoaded = RecurseInheritedTypes(Type, x => x.GetMethod("OnLoaded", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic, null, new Type[0], null));
                        if (fnOnLoaded != null)
                        {
                            il.Emit(OpCodes.Dup);
                            il.Emit(OpCodes.Callvirt, fnOnLoaded);
                        }
                    }

                    il.Emit(OpCodes.Ret);

                    // Cache it, return it
                    var del = m.CreateDelegate(Expression.GetFuncType(typeof(IDataReader), Type));
                    _pocoFactories.Add(key, del);
                    return del;
                }
                finally
                {
                    RwLock.ExitWriteLock();
                }
            }

            private static void AddConverterToStack(ILGenerator il, Func<object, object> converter)
            {
                if (converter != null)
                {
                    // Add the converter
                    var converterIndex = m_Converters.Count;
                    m_Converters.Add(converter);

                    // Generate IL to push the converter onto the stack
                    il.Emit(OpCodes.Ldsfld, fldConverters);
                    il.Emit(OpCodes.Ldc_I4, converterIndex);
                    il.Emit(OpCodes.Callvirt, fnListGetItem);					// Converter
                }
            }

            // ReSharper disable once UnusedParameter.Local
            private static Func<object, object> GetConverter(bool forceDateTimesToUtc, PocoColumn pc, Type srcType, Type dstType)
            {
                Func<object, object> converter = null;

                if (pc != null)
                {
                    var convertAttr = pc.PropertyInfo.GetCustomAttributes<ConverterAttribute>(true).FirstOrDefault();
                    if (convertAttr != null)
                    {
                        return convertAttr.GetConverter(srcType, dstType);
                    }
                }

                // Standard DateTime->Utc mapper
                if (forceDateTimesToUtc && srcType == typeof(DateTime) && (dstType == typeof(DateTime) || dstType == typeof(DateTime?)))
                {
                    converter = src => new DateTime(((DateTime)src).Ticks, DateTimeKind.Utc);
                }
                if (srcType.Name == "SqlHierarchyId")
                {
                    converter = src => src.ToString();
                }
                // Forced type conversion including integral types -> enum
                if (converter == null)
                {
                    if (dstType.IsEnum && IsIntegralType(srcType))
                    {
                        if (srcType != typeof(int))
                        {
                            converter = src => Convert.ChangeType(src, typeof(int), null);
                        }
                    }
                    else if (!dstType.IsAssignableFrom(srcType))
                    {
                        converter = src => Convert.ChangeType(src, dstType, null);
                    }
                }
                return converter;
            }

            static T RecurseInheritedTypes<T>(Type t, Func<Type, T> cb)
            {
                while (t != null)
                {
                    var info = cb(t);
                    if (info != null)
                        return info;
                    t = t.BaseType;
                }
                return default(T);
            }

            static readonly Dictionary<Type, PocoData> m_PocoDatas = new Dictionary<Type, PocoData>();
            static readonly List<Func<object, object>> m_Converters = new List<Func<object, object>>();
            static readonly MethodInfo fnGetValue = typeof(IDataRecord).GetMethod("GetValue", new[] { typeof(int) });
            static readonly MethodInfo fnIsDBNull = typeof(IDataRecord).GetMethod("IsDBNull");
            static readonly FieldInfo fldConverters = typeof(PocoData).GetField("m_Converters", BindingFlags.Static | BindingFlags.GetField | BindingFlags.NonPublic);
            static readonly MethodInfo fnListGetItem = typeof(List<Func<object, object>>).GetProperty("Item").GetGetMethod();
            static readonly MethodInfo fnInvoke = typeof(Func<object, object>).GetMethod("Invoke");
            public Type Type;
            public string[] QueryColumns { get; private set; }
            public TableInfo TableInfo { get; private set; }
            public Dictionary<string, PocoColumn> Columns { get; private set; }
            readonly Dictionary<string, Delegate> _pocoFactories = new Dictionary<string, Delegate>();
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

        readonly string _sql;
        readonly object[] _args;
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
            if (!string.IsNullOrEmpty(_sql))
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

        public Sql OrderBy(params string[] columns)
        {
            return Append(new Sql("ORDER BY " + string.Join(", ", columns.Select(c => c + " ASC "))));
        }

        public Sql OrderByDescending(params string[] columns)
        {
            return Append(new Sql("ORDER BY " + string.Join(", ", columns.Select(c => c + " DESC "))));
        }

        public Sql Select(params string[] columns)
        {
            return Append(new Sql("SELECT " + string.Join(", ", columns)));
        }

        public Sql From(params string[] tables)
        {
            return Append(new Sql("FROM " + string.Join(", ", tables)));
        }

        public Sql GroupBy(params string[] columns)
        {
            return Append(new Sql("GROUP BY " + string.Join(", ", columns)));
        }

        private SqlJoinClause Join(string joinType, string table)
        {
            return new SqlJoinClause(Append(new Sql(joinType + table)));
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

            public Sql Sql
            {
                get { return _sql; }
            }
        }

        public class SqlJoinClause<TLeft,TRight> : SqlJoinClause
        {
            private readonly string _alias;

            public SqlJoinClause(Sql sql)
                : base(sql)
            {
            }

            public SqlJoinClause(Sql sql, string alias)
                : base(sql)
            {
                _alias = alias;
            }

            public Sql<TLeft> On(Expression<Func<TLeft, TRight, bool>> predicate)
            {
                var aliasLeft = string.IsNullOrWhiteSpace(((Sql<TLeft>)Sql).Alias) ? Database.EscapeTableName(Database.PocoData.ForType(typeof(TLeft)).TableInfo.TableName) : ((Sql<TLeft>)Sql).Alias;
                var aliasRight = string.IsNullOrWhiteSpace(_alias) ? Database.EscapeTableName(Database.PocoData.ForType(typeof(TRight)).TableInfo.TableName) : _alias;
                var expressionVisitor = new OnClauseBuilder(aliasLeft, aliasRight);
                expressionVisitor.Visit(predicate);
                return (Sql<TLeft>)Sql.Append("ON " + expressionVisitor.Sql, expressionVisitor.Arguments);
            }
        }
    }

    public class Sql<T> : Sql
    {
        private readonly string _alias;

        public Sql()
        {
        }

        public Sql(string alias)
        {
            _alias = alias;
        }

        public string Alias
        {
            get { return _alias; }
        }

        public Sql<T> Where(Expression<Func<T, bool>> predicate)
        {
            var alias = string.IsNullOrWhiteSpace(_alias) ? Database.EscapeTableName(Database.PocoData.ForType(typeof(T)).TableInfo.TableName) : _alias;
            var expressionVisitor = new WhereClauseBuilder(alias);
            expressionVisitor.Visit(predicate);
            return (Sql<T>)Where(expressionVisitor.Sql, expressionVisitor.Arguments);
        }

        public Sql<T> Select(params Expression<Func<T, object>>[] keySelectors)
        {
            var alias = string.IsNullOrWhiteSpace(_alias) ? Database.EscapeTableName(Database.PocoData.ForType(typeof(T)).TableInfo.TableName) : _alias;
            if (keySelectors.Length==0)
            {
                return (Sql<T>)Select(Database.PocoData.ForType(typeof(T)).QueryColumns.Select(t => alias + "." + Database.EscapeSqlIdentifier(t)).ToArray());
            }
            return (Sql<T>)Select(keySelectors.Select(t => alias + "." + Database.EscapeSqlIdentifier(Database.GetColumnName(t))).ToArray());
        }

        public Sql<T> From()
        {
            return (Sql<T>)Append(new Sql("FROM " + Database.EscapeTableName(Database.PocoData.ForType(typeof(T)).TableInfo.TableName) + " " + (_alias??string.Empty) + " WITH(NOLOCK)"));
        }

        public Sql<T> OrderBy(params Expression<Func<T, object>>[] keySelectors)
        {
            var alias = string.IsNullOrWhiteSpace(_alias) ? Database.EscapeTableName(Database.PocoData.ForType(typeof(T)).TableInfo.TableName) : _alias;
            return (Sql<T>)OrderBy(keySelectors.Select(t => alias + "." + Database.EscapeSqlIdentifier(Database.GetColumnName(t))).ToArray());
        }

        public Sql<T> OrderByDescending(params Expression<Func<T, object>>[] keySelectors)
        {
            var alias = string.IsNullOrWhiteSpace(_alias) ? Database.EscapeTableName(Database.PocoData.ForType(typeof(T)).TableInfo.TableName) : _alias;
            return (Sql<T>)OrderByDescending(keySelectors.Select(t => alias + "." + Database.EscapeSqlIdentifier(Database.GetColumnName(t))).ToArray());
        }

        public SqlJoinClause<T, TRight> InnerJoin<TRight>(string alias = null)
        {
            var pd = Database.PocoData.ForType(typeof(TRight));
            return new SqlJoinClause<T, TRight>(Append(new Sql("INNER JOIN " + pd.TableInfo.TableName + " " + (alias ?? string.Empty) + " WITH(NOLOCK)")), alias);
        }

        public SqlJoinClause<T, TRight> LeftJoin<TRight>(string alias = null)
        {
            var pd = Database.PocoData.ForType(typeof(TRight));
            return new SqlJoinClause<T, TRight>(Append(new Sql("LEFT JOIN " + pd.TableInfo.TableName + " " + (alias ?? string.Empty) + " WITH(NOLOCK)")), alias);
        }
    }

    public class DataBaseException : Exception
    {
        public DataBaseException(string message,string sql):base(message)
        {
            Sql = sql;
        }

        public string Sql { get; private set; }
    }
}
