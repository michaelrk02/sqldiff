package internal

import (
	"database/sql"
	"fmt"
	"strings"
)

func In(x string, arr []string) bool {
	for _, y := range arr {
		if y == x {
			return true
		}
	}
	return false
}

func Escape(x string) string {
	x = strings.ReplaceAll(x, "'", "\\'")
	x = fmt.Sprintf("'%s'", x)
	return x
}

func Filter(input []string, fn func(v string) bool) []string {
	output := []string{}
	for _, x := range input {
		if fn(x) {
			output = append(output, x)
		}
	}
	return output
}

func NullInt(v sql.NullInt64) *int {
	if !v.Valid {
		return nil
	}
	x := int(v.Int64)
	return &x
}

func NullFloat(v sql.NullFloat64) *float64 {
	if !v.Valid {
		return nil
	}
	return &v.Float64
}

func NullString(v sql.NullString) *[]uint8 {
	if !v.Valid {
		return nil
	}
	x := []uint8(v.String)
	return &x
}
