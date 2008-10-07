﻿#region MIT license
// 
// MIT license
//
// Copyright (c) 2007-2008 Jiri Moudry, Pascal Craponne
// 
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
// 
#endregion

using System;
using System.Collections.Generic;
using System.Linq;
using DbLinq.Data.Linq.Sql;
using DbLinq.Vendor.Implementation;

namespace DbLinq.Ingres
{
    public class IngresSqlProvider : SqlProvider
    {
        public override SqlStatement GetInsertIds(IList<SqlStatement> outputParameters, IList<SqlStatement> outputExpressions)
        {
            // no parameters? no need to get them back
            if (outputParameters.Count == 0)
                return "";
            // otherwise we keep track of the new values
            return SqlStatement.Format("SELECT {0}",
                SqlStatement.Join(", ", (from outputExpression in outputExpressions
                                         select outputExpression.Replace("next value", "current value", true)).ToArray())
                );
        }

        protected override SqlStatement GetLiteralCount(SqlStatement a)
        {
            return "COUNT(*)";
        }

        protected override SqlStatement GetLiteralStringToLower(SqlStatement a)
        {
            return string.Format("LOWER({0})", a);
        }

        protected override SqlStatement GetLiteralStringToUpper(SqlStatement a)
        {
            return string.Format("UPPER({0})", a);
        }

        public override SqlStatement GetLiteralLimit(SqlStatement select, SqlStatement limit)
        {
            // return string.Format("SELECT FIRST {0} FROM ({1})", limit, select);
            var trimSelect = "SELECT ";
            if (select.Count > 0 && select[0].Sql.StartsWith(trimSelect))
            {
                var selectBuilder = new SqlStatementBuilder(select);
                var remaining = select[0].Sql.Substring(trimSelect.Length);
                selectBuilder.Parts[0] = new SqlLiteralPart(remaining);
                return SqlStatement.Format("SELECT FIRST {0} {1}", limit, selectBuilder.ToSqlStatement());
            }
            throw new ArgumentException("Invalid SELECT format");
        }

        public override SqlStatement GetLiteralLimit(SqlStatement select, SqlStatement limit, SqlStatement offset, SqlStatement offsetAndLimit)
        {
            // TODO: this is for you, Thomas...
            throw new NotImplementedException("OFFSET clause is not supported on Ingres");
        }

        public override string GetParameterName(string nameBase)
        {
            return "?";
        }

        protected override bool IsNameCaseSafe(string dbName)
        {
            return dbName == dbName.ToLower();
        }
    }
}