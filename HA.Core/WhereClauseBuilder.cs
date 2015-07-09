using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using System.Reflection;

namespace HA.Core
{
    public class WhereClauseBuilder<TModel> : ExpressionVisitor
    {
        private readonly StringBuilder _sb;

        private readonly string _alias;

        private readonly List<object> _args;

        public string Sql
        {
            get
            {
                return _sb.ToString();
            }
        }

        public object[] Arguments
        {
            get
            {
                return _args.ToArray();
            }
        }

        public WhereClauseBuilder()
        {
            _sb = new StringBuilder();
            _args = new List<object>();
        }

        public WhereClauseBuilder(string alias)
            : this()
        {
            _alias = alias;
        }

        protected override Expression VisitBinary(BinaryExpression node)
        {
            string operation;
            switch (node.NodeType)
            {
                case ExpressionType.Add:
                    operation = "+";
                    break;
                case ExpressionType.Equal:
                    operation = "=";
                    break;
                case ExpressionType.OrElse:
                    operation = "OR";
                    break;
                default:
                    throw new NotSupportedException(node.NodeType.ToString());
            }
            Visit(node.Left);
            _sb.Append(" ").Append(operation).Append(" ");
            Visit(node.Right);
            return node;
        }

        private object GetValue(MemberExpression node)
        {
            var objectMember = Expression.Convert(node, typeof(object));
            var getterLambda = Expression.Lambda<Func<object>>(objectMember);
            var getter = getterLambda.Compile();
            return getter.Invoke();
        }

        private object GetValue(MethodCallExpression node)
        {
            return Expression.Lambda(node).Compile().DynamicInvoke();
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Member.DeclaringType != typeof(TModel))
            {
                var constantExpr = Expression.Constant(GetValue(node));
                VisitConstant(constantExpr);
                return node;
            }
            var colAttr = node.Member.GetCustomAttribute<ColumnAttribute>(true);
            if (colAttr != null)
            {
                _sb.Append(_alias).Append(".").Append(Database.EscapeSqlIdentifier(colAttr.Name));
            }
            else
            {
                _sb.Append(_alias).Append(".").Append(Database.EscapeSqlIdentifier(node.Member.Name));
            }
            return node;
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            _sb.Append("@").Append(_args.Count);
            _args.Add(node.Value);
            return node;
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            var constantExpr = Expression.Constant(GetValue(node));
            VisitConstant(constantExpr);
            return node;
        }
    }
}
