using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
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
                {
                    return m.Value;
                }
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
        void AddParam(IDbCommand cmd, object item)
        {
            var p = cmd.CreateParameter();
            p.ParameterName = string.Format("@{0}", cmd.Parameters.Count);
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

        public string EscapeTableName(string str)
        {
            // Assume table names with "dot" are already escaped
            return str.IndexOf('.') >= 0 ? str : EscapeSqlIdentifier(str);
        }

        public string EscapeSqlIdentifier(string str)
        {
            return string.Format("[{0}]", str);
        }

        string AddSelectClause<T>(string sql)
        {
            if (sql.StartsWith(";"))
                return sql.Substring(1);

            if (!rxSelect.IsMatch(sql))
            {
                var pd = PocoData.ForType(typeof(T));
                var tableName = EscapeTableName(pd.TableInfo.TableName);
                string cols = string.Join(", ", (from c in pd.QueryColumns select tableName + "." + EscapeSqlIdentifier(c)).ToArray());
                if (!rxFrom.IsMatch(sql))
                    sql = string.Format("SELECT {0} FROM {1} {2}", cols, tableName, sql);
                else
                    sql = string.Format("SELECT {0} {1}", cols, sql);
            }
            return sql;
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
                for (int i = 0; i < args.Length; i++)
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
            public virtual void SetValue(object target, object val) { PropertyInfo.SetValue(target, val, null); }
            public virtual object GetValue(object target) { return PropertyInfo.GetValue(target, null); }
            public virtual object ChangeType(object val) { return Convert.ChangeType(val, PropertyInfo.PropertyType); }
        }

        public class ExpandoColumn : PocoColumn
        {
            public override void SetValue(object target, object val) { (target as IDictionary<string, object>)[ColumnName] = val; }
            public override object GetValue(object target)
            {
                object val = null;
                (target as IDictionary<string, object>).TryGetValue(ColumnName, out val);
                return val;
            }
            public override object ChangeType(object val) { return val; }
        }

        public class PocoData
        {
            public PocoData()
            {
            }

            public static PocoData ForObject(object o, string primaryKeyName)
            {
                var t = o.GetType();
                if (t == typeof(System.Dynamic.ExpandoObject))
                {
                    var pd = new PocoData();
                    pd.TableInfo = new TableInfo();
                    pd.Columns = new Dictionary<string, PocoColumn>(StringComparer.OrdinalIgnoreCase);
                    pd.Columns.Add(primaryKeyName, new ExpandoColumn() { ColumnName = primaryKeyName });
                    pd.TableInfo.PrimaryKey = primaryKeyName;
                    pd.TableInfo.AutoIncrement = true;
                    foreach (var col in (o as IDictionary<string, object>).Keys)
                    {
                        if (col != primaryKeyName)
                            pd.Columns.Add(col, new ExpandoColumn() { ColumnName = col });
                    }
                    return pd;
                }
                else
                {
                    return ForType(t);
                }
            }
            static System.Threading.ReaderWriterLockSlim RWLock = new System.Threading.ReaderWriterLockSlim();
            public static PocoData ForType(Type t)
            {
                // Check cache
                RWLock.EnterReadLock();
                PocoData pd;
                try
                {
                    if (m_PocoDatas.TryGetValue(t, out pd))
                        return pd;
                }
                finally
                {
                    RWLock.ExitReadLock();
                }

                // Cache it
                RWLock.EnterWriteLock();
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
                    RWLock.ExitWriteLock();
                }
                return pd;
            }

            public PocoData(Type t)
            {
                type = t;
                TableInfo = new TableInfo();

                // Get the table name
                var tableNameAtt = t.GetCustomAttributes<TableNameAttribute>(true).FirstOrDefault();
                TableInfo.TableName = tableNameAtt == null ? t.Name : tableNameAtt.Value;

                // Get the primary key
                var primaryKeyAttr = t.GetCustomAttributes<PrimaryKeyAttribute>(true).FirstOrDefault();
                TableInfo.PrimaryKey = primaryKeyAttr == null ? "ID" : primaryKeyAttr.Value;
                TableInfo.AutoIncrement = primaryKeyAttr == null ? false : primaryKeyAttr.AutoIncrement;

                Columns = new Dictionary<string, PocoColumn>(StringComparer.OrdinalIgnoreCase);
                foreach (var pi in t.GetProperties())
                {
                    // Work out if properties is to be included
                    var colAttr = pi.GetCustomAttributes<ColumnAttribute>(true).FirstOrDefault();
                    if (colAttr == null)
                        continue;

                    var pc = new PocoColumn();
                    pc.PropertyInfo = pi;

                    // Work out the DB column name
                    pc.ColumnName = colAttr.Name;
                    if ((colAttr as ResultColumnAttribute) != null)
                        pc.ResultColumn = true;

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
            public Delegate GetFactory(string sql, string connString, bool ForceDateTimesToUtc, int firstColumn, int countColumns, IDataReader r)
            {
                // Check cache
                var key = string.Format("{0}:{1}:{2}:{3}:{4}", sql, connString, ForceDateTimesToUtc, firstColumn, countColumns);
                RWLock.EnterReadLock();
                try
                {
                    // Have we already created it?
                    Delegate factory;
                    if (PocoFactories.TryGetValue(key, out factory))
                        return factory;
                }
                finally
                {
                    RWLock.ExitReadLock();
                }

                // Take the writer lock
                RWLock.EnterWriteLock();

                try
                {
                    // Check again, just in case
                    Delegate factory;
                    if (PocoFactories.TryGetValue(key, out factory))
                        return factory;

                    // Create the method
                    var m = new DynamicMethod("petapoco_factory_" + PocoFactories.Count.ToString(), type, new Type[] { typeof(IDataReader) }, true);
                    var il = m.GetILGenerator();

                    if (type == typeof(object))
                    {
                        // var poco=new T()
                        il.Emit(OpCodes.Newobj, typeof(System.Dynamic.ExpandoObject).GetConstructor(Type.EmptyTypes));			// obj

                        MethodInfo fnAdd = typeof(IDictionary<string, object>).GetMethod("Add");

                        // Enumerate all fields generating a set assignment for the column
                        for (int i = firstColumn; i < firstColumn + countColumns; i++)
                        {
                            var srcType = r.GetFieldType(i);

                            il.Emit(OpCodes.Dup);						// obj, obj
                            il.Emit(OpCodes.Ldstr, r.GetName(i));		// obj, obj, fieldname

                            // Get the converter
                            Func<object, object> converter = null;

                            if (ForceDateTimesToUtc && converter == null && srcType == typeof(DateTime))
                                converter = delegate(object src) { return new DateTime(((DateTime)src).Ticks, DateTimeKind.Utc); };

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
                    else if (type.IsValueType || type == typeof(string) || type == typeof(byte[]))
                    {
                        // Do we need to install a converter?
                        var srcType = r.GetFieldType(0);
                        var converter = GetConverter(ForceDateTimesToUtc, null, srcType, type);

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
                        il.Emit(OpCodes.Unbox_Any, type);								// value converted
                    }
                    else
                    {
                        // var poco=new T()
                        il.Emit(OpCodes.Newobj, type.GetConstructor(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic, null, new Type[0], null));

                        // Enumerate all fields generating a set assignment for the column
                        for (int i = firstColumn; i < firstColumn + countColumns; i++)
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
                            var converter = GetConverter(ForceDateTimesToUtc, pc, srcType, dstType);

                            // Fast
                            bool Handled = false;
                            if (converter == null)
                            {
                                var valuegetter = typeof(IDataRecord).GetMethod("Get" + srcType.Name, new Type[] { typeof(int) });
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
                                        il.Emit(OpCodes.Newobj, dstType.GetConstructor(new Type[] { Nullable.GetUnderlyingType(dstType) }));
                                    }

                                    il.Emit(OpCodes.Callvirt, pc.PropertyInfo.GetSetMethod(true));		// poco
                                    Handled = true;
                                }
                            }

                            // Not so fast
                            if (!Handled)
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

                        var fnOnLoaded = RecurseInheritedTypes<MethodInfo>(type, (x) => x.GetMethod("OnLoaded", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic, null, new Type[0], null));
                        if (fnOnLoaded != null)
                        {
                            il.Emit(OpCodes.Dup);
                            il.Emit(OpCodes.Callvirt, fnOnLoaded);
                        }
                    }

                    il.Emit(OpCodes.Ret);

                    // Cache it, return it
                    var del = m.CreateDelegate(Expression.GetFuncType(typeof(IDataReader), type));
                    PocoFactories.Add(key, del);
                    return del;
                }
                finally
                {
                    RWLock.ExitWriteLock();
                }
            }

            private static void AddConverterToStack(ILGenerator il, Func<object, object> converter)
            {
                if (converter != null)
                {
                    // Add the converter
                    int converterIndex = m_Converters.Count;
                    m_Converters.Add(converter);

                    // Generate IL to push the converter onto the stack
                    il.Emit(OpCodes.Ldsfld, fldConverters);
                    il.Emit(OpCodes.Ldc_I4, converterIndex);
                    il.Emit(OpCodes.Callvirt, fnListGetItem);					// Converter
                }
            }

            private static Func<object, object> GetConverter(bool forceDateTimesToUtc, PocoColumn pc, Type srcType, Type dstType)
            {
                Func<object, object> converter = null;

                // Standard DateTime->Utc mapper
                if (forceDateTimesToUtc && converter == null && srcType == typeof(DateTime) && (dstType == typeof(DateTime) || dstType == typeof(DateTime?)))
                {
                    converter = delegate(object src) { return new DateTime(((DateTime)src).Ticks, DateTimeKind.Utc); };
                }

                // Forced type conversion including integral types -> enum
                if (converter == null)
                {
                    if (dstType.IsEnum && IsIntegralType(srcType))
                    {
                        if (srcType != typeof(int))
                        {
                            converter = delegate(object src) { return Convert.ChangeType(src, typeof(int), null); };
                        }
                    }
                    else if (!dstType.IsAssignableFrom(srcType))
                    {
                        converter = delegate(object src) { return Convert.ChangeType(src, dstType, null); };
                    }
                }
                return converter;
            }

            static T RecurseInheritedTypes<T>(Type t, Func<Type, T> cb)
            {
                while (t != null)
                {
                    T info = cb(t);
                    if (info != null)
                        return info;
                    t = t.BaseType;
                }
                return default(T);
            }

            static Dictionary<Type, PocoData> m_PocoDatas = new Dictionary<Type, PocoData>();
            static List<Func<object, object>> m_Converters = new List<Func<object, object>>();
            static MethodInfo fnGetValue = typeof(IDataRecord).GetMethod("GetValue", new Type[] { typeof(int) });
            static MethodInfo fnIsDBNull = typeof(IDataRecord).GetMethod("IsDBNull");
            static FieldInfo fldConverters = typeof(PocoData).GetField("m_Converters", BindingFlags.Static | BindingFlags.GetField | BindingFlags.NonPublic);
            static MethodInfo fnListGetItem = typeof(List<Func<object, object>>).GetProperty("Item").GetGetMethod();
            static MethodInfo fnInvoke = typeof(Func<object, object>).GetMethod("Invoke");
            public Type type;
            public string[] QueryColumns { get; private set; }
            public TableInfo TableInfo { get; private set; }
            public Dictionary<string, PocoColumn> Columns { get; private set; }
            Dictionary<string, Delegate> PocoFactories = new Dictionary<string, Delegate>();
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
