using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq.Expressions;
using HA.Model.Foundation;
using HA.Service.Foundation;
using System.Linq;
using System.Reflection;
using System.Text;
using HA.Core;

namespace HA.Console
{

    public class DatabaseQueryable<T> : IQueryable<T>
    {
        public DatabaseQueryable(IQueryProvider provider)
        {
            Provider = provider;
            Expression = Expression.Constant(this);
        }

        public DatabaseQueryable(IQueryProvider provider, Expression expression)
        {
            Provider = provider;
            Expression = expression;
        }

        public IEnumerator<T> GetEnumerator()
        {
            return Provider.Execute<IEnumerable<T>>(Expression).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            throw new NotImplementedException();
        }

        public Type ElementType
        {
            get { return typeof(T); }
        }

        public Expression Expression { get; private set; }

        public IQueryProvider Provider { get; private set; }
    }

    public class DatabaseQueryProvider :IQueryProvider
    {
        private readonly SqlWriter _sqlWriter;

        public DatabaseQueryProvider()
        {
            _sqlWriter = new SqlWriter();
        }

        public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            return new DatabaseQueryable<TElement>(this, expression);
        }

        public IQueryable CreateQuery(Expression expression)
        {
            throw new NotImplementedException();
        }

        public TResult Execute<TResult>(Expression expression)
        {
            StringBuilder sql;
            List<object> args;
            _sqlWriter.Writer(expression, out  sql, out args);
            return default(TResult);
        }

        public object Execute(Expression expression)
        {
            throw new NotImplementedException();
        }
    }

    public class SqlWriter : ExpressionVisitor
    {
        private StringBuilder _sql;
        private List<object> _args;

        public void Writer(Expression node, out StringBuilder sql, out List<object> args)
        {
            _sql = new StringBuilder();
            _args = new List<object>();
            Visit(node);
            sql = _sql;
            args = _args;
        }

        protected override Expression VisitBinary(BinaryExpression node)
        {
            string operation;
            switch (node.NodeType)
            {
                case ExpressionType.Equal:
                    operation = "=";
                    break;
                case ExpressionType.LessThan:
                    operation = "<";
                    break;
                case ExpressionType.LessThanOrEqual:
                    operation = "<=";
                    break;
                case ExpressionType.GreaterThan:
                    operation = ">";
                    break;
                case ExpressionType.GreaterThanOrEqual:
                    operation = ">=";
                    break;
                case ExpressionType.NotEqual:
                    operation = "!=";
                    break;
                case ExpressionType.OrElse:
                    operation = "OR";
                    break;
                case ExpressionType.And:
                case ExpressionType.AndAlso:
                    operation = "AND";
                    break;
                default:
                    throw new NotSupportedException(node.NodeType.ToString());
            }
            VisitLeft(node.Left);
            _sql.Append(" ").Append(operation).Append(" ");
            VisitRight(node.Right);
            return node;
        }

        private void VisitLeft(Expression node)
        {
            Visit(node);
        }

        private void VisitRight(Expression node)
        {
            switch (node.NodeType)
            {
                case ExpressionType.MemberAccess:
                    VisitConstant(Expression.Constant(GetValue((MemberExpression)node)));
                    break;
                case ExpressionType.Constant:
                    VisitConstant((ConstantExpression)node);
                    break;
                default:
                    Visit(node);
                    break;
            }
        }

        private static object GetValue(MemberExpression node)
        {
            var objectMember = Expression.Convert(node, typeof(object));
            var getterLambda = Expression.Lambda<Func<object>>(objectMember);
            var getter = getterLambda.Compile();
            return getter.Invoke();
        }

        private static object GetValue(MethodCallExpression node)
        {
            return Expression.Lambda(node).Compile().DynamicInvoke();
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            if(node.Value is IQueryable)
            {
                return node;
            }
            if (node.Value is IEnumerable && !(node.Value is string) && !(node.Value is byte[]))
            {
                _sql.Append("( @").Append(_args.Count).Append(" )");
            }
            else
            {
                _sql.Append("@").Append(_args.Count);
            }
            _args.Add(node.Value);
            return node;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            var colAttr = node.Member.GetCustomAttribute<ColumnAttribute>(true);
            _sql.Append(Database.EscapeSqlIdentifier(colAttr != null ? colAttr.Name : node.Member.Name));
            return node;
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            switch (node.Method.Name)
            {
                case "Where":
                    if (node.Arguments.Count > 1)
                    {
                        Visit(node.Arguments);
                    }
                    else
                    {
                        Visit(node.Arguments[0]);
                    }
                    break;
            }
            return node;
        }

        protected override Expression VisitUnary(UnaryExpression node)
        {
            Visit(node.Operand);
            return node;
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            var query = new DatabaseQueryable<BaseDataDataModel>(new DatabaseQueryProvider());
            var list = query.Where(t => t.Name == "edward"||t.CreatedOn==DateTime.Now).Where(t=>t.Name!="pp").ToList();
            //EnumerableQuery<string>
            //Insert();

            //Update();
            var p=new List<int>().AsQueryable().Where(t => t > 0).ToList();
            //var db=new Database("HA");
            //var list = db.Page<BaseDataDataModel>(1, 1, new Sql<BaseDataDataModel>().Where(t => t.RowStatus == 0));

            //var model = new BaseDataDataModel { Id = 1000,Path="/1/" };
            //var num =Convert.ToInt32(db.Insert(model));
            //db.Insert(new List<BaseDataDataModel> { model });
            //db.Update(model);
            //db.Update(model, t => t.Name, t => t.GroupCode);
            //db.BulkUpdate(new List<BaseDataDataModel> { model }, t => t.Name);
            //Page();


        }

        private static void Page()
        {
            var service = new BaseDataService();
            var list = service.Fetch(new Sql<BaseDataDataModel>("T").Select(t=>t.Name).From().Where(t => t.RowStatus == 0 && t.Id > 10).OrderBy(t=>t.CreatedOn,t=>t.RowStatus).OrderByDescending(t=>t.Name));
            var page = service.Page(2, 15, new Sql<BaseDataDataModel>().Where(t => t.RowStatus == 0 && t.Id > 10));
        }

        private static void Update()
        {
            var service = new BaseDataService();
            var list = service.Fetch(new Sql<BaseDataDataModel>().Where(t => t.RowStatus == 0));
            service.Update(list[0]);
            service.Update(list[0], t => t.RowStatus);
            service.Update(new Expression<Func<BaseDataDataModel, object>>[]{t => t.Name, t => t.GroupCode}, list, t => t.RowStatus, t => t.Code);
        }

        private static void Insert()
        {
            var service = new BaseDataService();
            var list = new List<BaseDataDataModel>
            {
                new BaseDataDataModel {GroupCode = "X", Code = "1", Name = "edward4", CreatedBy = "edward"},
                new BaseDataDataModel {GroupCode = "X", Code = "1", Name = "edward5", CreatedBy = "edward"}
            };
            var list2 = new List<BaseDataDataModel>();
            //list2.Add(new BaseDataDataModel { GroupCode = "Y", Code = "1", Name = "edward4", CreatedBy = "edward" });
            //list2.Add(new BaseDataDataModel { GroupCode = "Y", Code = "1", Name = "edward5", CreatedBy = "edward" });
            var listGroup = new List<BaseDataGroupDataModel>
            {
                new BaseDataGroupDataModel {Code = "A",CreatedOn=DateTime.Now, BaseDataList = list},
                new BaseDataGroupDataModel {Code = "B", BaseDataList = list2}
            };
            service.Insert(listGroup);
        }
    }
}
