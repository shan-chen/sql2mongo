package sql2couchdb

import (
	"errors"
	"fmt"
	"github.com/xwb1989/sqlparser"
	"strings"
)

// currently only support select

func Parse(sql string) (string,string,error){
	stmt, err := sqlparser.Parse(sql)

	if err != nil {
		fmt.Println(err)
	}

	var docSQL string
	var tableName string
	switch stmt.(type) {
	case *sqlparser.Select:
		docSQL, tableName, err = handleSelect(stmt.(*sqlparser.Select))
	case *sqlparser.Update, *sqlparser.Insert, *sqlparser.Delete:
		return "","",errors.New("action type is not supported")
	}

	if err != nil {
		return "","",nil
	}
	return docSQL,tableName,nil
}

func handleSelect(sel *sqlparser.Select) (string, string, error) {
	var rootParent sqlparser.Expr
	if len(sel.From) != 1 {
		return "", "", errors.New("does not support multiple from")
	}

	tableName := strings.Replace(sqlparser.String(sel.From), "`", "", -1)
	resultMap := make(map[string]interface{})

	// where
	if sel.Where != nil {
		selectorStr, err := handleSelectWhere(&sel.Where.Expr, true, &rootParent)
		if err != nil {
			return "", tableName, err
		}
		resultMap["selector"] = selectorStr
	}

	// limit
	if sel.Limit != nil {
		skip := "0"
		if sel.Limit.Offset != nil {
			skip = sqlparser.String(sel.Limit.Offset)
		}
		limit := sqlparser.String(sel.Limit.Rowcount)
		resultMap["skip"] = skip
		resultMap["limit"] = limit
	}

	// order
	var orderByArr []string
	for _, orderByExpr := range sel.OrderBy {
		orderByStr := fmt.Sprintf(`{"%v": "%v"}`, strings.Replace(sqlparser.String(orderByExpr.Expr), "`", "", -1), orderByExpr.Direction)
		orderByArr = append(orderByArr, orderByStr)
	}
	if len(orderByArr) > 0 {
		resultMap["sort"] = fmt.Sprintf("[%v]", strings.Join(orderByArr, ","))
	}

	filterKeys := []string{"selector", "sort", "skip", "limit"}
	resultArr := make([]string, 0)
	for _, key := range filterKeys {
		if v, ok := resultMap[key]; ok {
			resultArr = append(resultArr, fmt.Sprintf("%v:%v", key, v))
		}
	}
	return fmt.Sprintf("{%v}", strings.Join(resultArr, ",")), tableName, nil
}

func handleSelectWhere(expr *sqlparser.Expr, topLevel bool, parent *sqlparser.Expr) (string, error) {
	if expr == nil {
		return "", errors.New("error expression cannot be nil here")
	}

	switch (*expr).(type) {
	case *sqlparser.AndExpr:
		return handleSelectWhereAndExpr(expr, topLevel, parent)

	case *sqlparser.OrExpr:
		return handleSelectWhereOrExpr(expr, topLevel, parent)
	case *sqlparser.ComparisonExpr:
		return handleSelectWhereComparisonExpr(expr, topLevel, parent)

	case *sqlparser.RangeCond:
		//TODO 支持between
		// between a and b
		// the meaning is equal to range query
		/*
			rangeCond := (*expr).(*sqlparser.RangeCond)
			colName, ok := rangeCond.Left.(*sqlparser.ColName)

			if !ok {
				return "", errors.New("elasticsql: range column name missing")
			}

			colNameStr := sqlparser.String(colName)
			fromStr := strings.Trim(sqlparser.String(rangeCond.From), `'`)
			toStr := strings.Trim(sqlparser.String(rangeCond.To), `'`)

			resultStr := fmt.Sprintf(`{"range" : {"%v" : {"from" : "%v", "to" : "%v"}}}`, colNameStr, fromStr, toStr)
			if topLevel {
				resultStr = fmt.Sprintf(`{"bool" : {"must" : [%v]}}`, resultStr)
			}

			return resultStr, nil
		*/

	case *sqlparser.ParenExpr:
		parentBoolExpr := (*expr).(*sqlparser.ParenExpr)
		boolExpr := parentBoolExpr.Expr

		// if paren is the top level, bool must is needed
		var isThisTopLevel = false
		if topLevel {
			isThisTopLevel = true
		}
		return handleSelectWhere(&boolExpr, isThisTopLevel, parent)

	default:
		return "", errors.New("grammer is not supported")
	}
	return "", nil
}

func handleSelectWhereAndExpr(expr *sqlparser.Expr, topLevel bool, parent *sqlparser.Expr) (string, error) {
	andExpr := (*expr).(*sqlparser.AndExpr)
	leftExpr := andExpr.Left
	rightExpr := andExpr.Right
	leftStr, err := handleSelectWhere(&leftExpr, false, expr)
	if err != nil {
		return "", err
	}
	rightStr, err := handleSelectWhere(&rightExpr, false, expr)
	if err != nil {
		return "", err
	}

	var resultStr string
	if leftStr == "" || rightStr == "" {
		resultStr = leftStr + rightStr
	} else {
		resultStr = leftStr + `,` + rightStr
	}

	if _, ok := (*parent).(*sqlparser.AndExpr); ok {
		return resultStr, nil
	}
	return fmt.Sprintf(`{"$and": [%v]}`, resultStr), nil
}

func handleSelectWhereOrExpr(expr *sqlparser.Expr, topLevel bool, parent *sqlparser.Expr) (string, error) {
	orExpr := (*expr).(*sqlparser.OrExpr)
	leftExpr := orExpr.Left
	rightExpr := orExpr.Right

	leftStr, err := handleSelectWhere(&leftExpr, false, expr)
	if err != nil {
		return "", err
	}

	rightStr, err := handleSelectWhere(&rightExpr, false, expr)
	if err != nil {
		return "", err
	}

	var resultStr string
	if leftStr == "" || rightStr == "" {
		resultStr = leftStr + rightStr
	} else {
		resultStr = leftStr + `,` + rightStr
	}

	if _, ok := (*parent).(*sqlparser.OrExpr); ok {
		return resultStr, nil
	}

	return fmt.Sprintf(`{"$or": [%v]}`, resultStr), nil
}

func handleSelectWhereComparisonExpr(expr *sqlparser.Expr, topLevel bool, parent *sqlparser.Expr) (string, error) {
	comparisonExpr := (*expr).(*sqlparser.ComparisonExpr)
	colName, ok := comparisonExpr.Left.(*sqlparser.ColName)

	if !ok {
		return "", errors.New("invalid comparison expression, the left must be a column name")
	}

	colNameStr := sqlparser.String(colName)
	colNameStr = strings.Replace(colNameStr, "`", "", -1)
	rightStr, missingCheck, err := buildComparisonExprRightStr(comparisonExpr.Right)
	if err != nil {
		return "", err
	}
	if missingCheck {
		return "", errors.New("sql missing field")
	}
	resultStr := ""

	switch comparisonExpr.Operator {
	case ">=":
		resultStr = fmt.Sprintf(`{"%v" : {"$gte" : "%v"}}`, colNameStr, rightStr)
	case "<=":
		resultStr = fmt.Sprintf(`{"%v" : {"$lte" : "%v"}}`, colNameStr, rightStr)
	case "=":
		resultStr = fmt.Sprintf(`{"%v": {"$eq" : "%v"}}`, colNameStr, rightStr)
	case ">":
		resultStr = fmt.Sprintf(`{"%v" : {"$gt" : "%v"}}`, colNameStr, rightStr)
	case "<":
		resultStr = fmt.Sprintf(`{"%v" : {"$lt" : "%v"}}`, colNameStr, rightStr)
	case "!=":
		resultStr = fmt.Sprintf(`{"%v" : {"$ne" : "%v"}}`, colNameStr, rightStr)
	case "in":
		// the default valTuple is ('1', '2', '3') like
		rightStr = strings.Replace(rightStr, `'`, `"`, -1)
		rightStr = strings.Trim(rightStr, "(")
		rightStr = strings.Trim(rightStr, ")")
		resultStr = fmt.Sprintf(`{"%v" : {"$in" : [%v]}}`, colNameStr, rightStr)
	case "not in":
		rightStr = strings.Replace(rightStr, `'`, `"`, -1)
		rightStr = strings.Trim(rightStr, "(")
		rightStr = strings.Trim(rightStr, ")")
		resultStr = fmt.Sprintf(`{"$not":{"%v" : {"$in" : [%v]}}}`, colNameStr, rightStr)
	case "like":
		rightStr = strings.Replace(rightStr, `%`, `.*`, -1)
		resultStr = fmt.Sprintf(`{"%v" : {"$regex" : "%v"}}}`, colNameStr, rightStr)
	case "not like":
		rightStr = strings.Replace(rightStr, `%`, `.*`, -1)
		resultStr = fmt.Sprintf(`{"not" : {"%v" : {"$regex" : "%v"}}}`, colNameStr, rightStr)
	}

	return resultStr, nil
}

func buildComparisonExprRightStr(expr sqlparser.Expr) (string, bool, error) {
	var rightStr string
	var err error
	missingCheck := false
	switch expr.(type) {
	case *sqlparser.SQLVal:
		rightStr = sqlparser.String(expr)
		rightStr = strings.Trim(rightStr, `'`)
	case *sqlparser.GroupConcatExpr:
		return "", false, errors.New("does not support group_concat")
	case *sqlparser.FuncExpr:
		// parse nested
		//funcExpr := expr.(*sqlparser.FuncExpr)
		//rightStr, err = buildNestedFuncStrValue(funcExpr)
		//if err != nil {
		//	return "", missingCheck, err
		//}
		return "", false, errors.New("does not support nested")

	case *sqlparser.ColName:
		if sqlparser.String(expr) == "missing" {
			missingCheck = true
			return "", missingCheck, nil
		}

		return "", missingCheck, errors.New("column name on the right side of compare operator is not supported")
	case sqlparser.ValTuple:
		rightStr = sqlparser.String(expr)
	default:
		// cannot reach here
	}
	return rightStr, missingCheck, err
}
