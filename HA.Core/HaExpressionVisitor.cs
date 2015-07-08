using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace HA.Core
{
    public class HaExpressionVisitor : ExpressionVisitor
    {
        private readonly StringBuilder _sb;

        public HaExpressionVisitor()
        {
            _sb = new StringBuilder();
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
            _sb.Append("(");
            Visit(node.Left);
            _sb.Append(" ").Append(operation).Append(" ");
            Visit(node.Right);
            _sb.Append(")"); ;
            return node;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            return node;
        }
    }
}
