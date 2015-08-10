using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Linq;

namespace HA.Core
{
    public static class ExtensionClass
    {
        public static bool ExistsIn<T>(this T item, IEnumerable<T> collection)
        {
            return collection.Contains(item);
        }
    }

    public class WhereClauseBuilder : ExpressionVisitor
    {
        private readonly StringBuilder _sb;
        private readonly string _alias;
        private readonly List<object> _args;
        private string _methodName;

        public string MethodName
        {
            get
            {
                var methodName = _methodName;
                _methodName = null;
                return methodName;
            }
            set
            {
                _methodName = value;
            }
        }

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
            if (node.NodeType == ExpressionType.AndAlso || node.NodeType == ExpressionType.OrElse)
            {
                _sb.Append("(");
            }
            VisitLeft(node.Left);
            _sb.Append(" ").Append(operation).Append(" ");
            VisitRight(node.Right);
            if (node.NodeType == ExpressionType.AndAlso || node.NodeType == ExpressionType.OrElse)
            {
                _sb.Append(")");
            }
            return node;
        }

        protected virtual void VisitLeft(Expression node)
        {
            Visit(node);
        }

        protected override Expression VisitUnary(UnaryExpression node)
        {
            if (node.NodeType == ExpressionType.Not)
            {
                _sb.Append("NOT ");
            }
            Visit(node.Operand);
            return node;
        }

        protected virtual void VisitRight(Expression node)
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

        protected override Expression VisitMember(MemberExpression node)
        {
            var colAttr = node.Member.GetCustomAttribute<ColumnAttribute>(true);
            _sb.Append(_alias).Append(".").Append(Database.EscapeSqlIdentifier(colAttr != null ? colAttr.Name : node.Member.Name));
            return node;
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            if (node.Value is IEnumerable && !(node.Value is string) && !(node.Value is byte[]))
            {
                _sb.Append("( @").Append(_args.Count).Append(" )");
            }
            else
            {
                _sb.Append("@").Append(_args.Count);
            }
            switch (MethodName)
            {
                case "StartsWith":
                    _args.Add(node.Value + "%");
                    break;
                case "EndsWith":
                    _args.Add("%" + node.Value);
                    break;
                case "Contains":
                    _args.Add("%" + node.Value + "%");
                    break;
                default:
                    _args.Add(node.Value);
                    break;
            }
            return node;
        }

        protected override Expression VisitListInit(ListInitExpression node)
        {
            return node;
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            MethodName = node.Method.Name;
            switch (node.Method.Name)
            {
                case "Equals":
                    VisitLeft(node.Object);
                    _sb.Append(" = ");
                    VisitRight(node.Arguments[0]);
                    return node;
                case "ExistsIn":
                    VisitLeft(node.Arguments[0]);
                    _sb.Append(" IN ");
                    VisitRight(node.Arguments[1]);
                    return node;
                case "StartsWith":
                    VisitLeft(node.Object);
                    _sb.Append(" LIKE ");
                    VisitRight(node.Arguments[0]);
                    return node;
                case "EndsWith":
                    VisitLeft(node.Object);
                    _sb.Append(" LIKE ");
                    VisitRight(node.Arguments[0]);
                    return node;
                case "Contains":
                    VisitLeft(node.Object);
                    _sb.Append(" LIKE ");
                    VisitRight(node.Arguments[0]);
                    return node;
                default:
                    return VisitConstant(Expression.Constant(GetValue(node)));
            }
        }
    }
}
