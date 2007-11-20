#region MIT license
////////////////////////////////////////////////////////////////////
// MIT license:
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
//
// Authors:
//        Jiri George Moudry
////////////////////////////////////////////////////////////////////
#endregion

using System;
using System.Linq;
using System.Linq.Expressions;
using System.Collections.Generic;
using System.Text;
using DBLinq.Linq.clause;
using DBLinq.util;

namespace DBLinq.Linq
{
    /// <summary>
    /// holds queries as they are passed in via MTable.CreateQuery() and MTable.Execute()
    /// </summary>
    public class SessionVars
    {
        static int                      s_serial = 0;
        public readonly int             _serial = s_serial++;

        public readonly MContext        context;


        /// <summary>
        /// chain of expressions processed so far. 
        /// for db.Employees.Where(...).Select(...), we would hold 2 expressions.
        /// </summary>
        public readonly List<Expression> expressionChain = new List<Expression>();
        
        /// <summary>
        /// optional scalar expression, terminating the chain
        /// </summary>
        public Expression scalarExpression;

        public SessionVars(MContext context)
        {
            this.context = context;
        }

        /// <summary>
        /// clone existing SessionVars and add latest Expression.
        /// </summary>
        public SessionVars(SessionVars vars)
        { 
            context = vars.context;
            expressionChain = new List<Expression>(vars.expressionChain);
            scalarExpression = vars.scalarExpression;
        }
        public SessionVars Add(Expression expressionToAdd)
        {
            expressionChain.Add(expressionToAdd);
            return this;
        }
        public SessionVars AddScalar(Expression expression)
        {
            scalarExpression = expression;
            return this;
        }
    }

    /// <summary>
    /// the 'finalized' SessionVars.
    /// (meaning expressions have been parsed, after enumeration has started).
    /// 
    /// You create an instance via QueryProcessor.ProcessLambdas()
    /// </summary>
    public sealed class SessionVarsParsed : SessionVars
    {
        /// <summary>
        /// components of SQL expression (where clause, order, select ...)
        /// </summary>
        public SqlExpressionParts _sqlParts = new SqlExpressionParts();
        
        public LambdaExpression groupByExpr;
        public LambdaExpression groupByNewExpr;

        /// <summary>
        /// list of reflected fields - this will be used to compile a row reader method
        /// </summary>
        public ProjectionData   projectionData;

        /// <summary>
        /// in SelectMany, there is mapping c.Orders => o
        /// </summary>
        public Dictionary<MemberExpression,string> memberExprNickames = new Dictionary<MemberExpression,string>();

        /// <summary>
        /// created by post-processing in QueryProcessor.build_SQL_string(), used in RowEnumerator
        /// </summary>
        public string sqlString;


        public SessionVarsParsed(SessionVars vars)
            : base(vars)
        {
        }


    }
}
