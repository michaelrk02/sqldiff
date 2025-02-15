package internal

import (
	"fmt"
	"strings"
)

type Record struct {
	Columns []string
	Data    map[string]interface{}
}

func NewRecord(columns []string) *Record {
	return &Record{
		Columns: columns,
		Data:    make(map[string]interface{}),
	}
}

func (r *Record) InsertSql(table string) string {
	attributePairs := NewAttributePairs()

	for _, k := range r.Columns {
		attributePairs.Add(Attribute(k), r.Value(k))
	}

	return fmt.Sprintf(
		"INSERT INTO `%s` (%s) VALUES (%s)",
		table,
		attributePairs.InsertColumns(),
		attributePairs.InsertValues(),
	)
}

func (r *Record) UpdateSql(table string, primaryKeys, attributes []string) string {
	attributePairs := NewAttributePairs()
	for _, k := range attributes {
		attributePairs.Add(Attribute(k), r.Value(k))
	}

	identifierPairs := NewAttributePairs()
	for _, k := range primaryKeys {
		identifierPairs.Add(Attribute(k), r.Value(k))
	}

	return fmt.Sprintf(
		"UPDATE `%s` SET %s WHERE %s",
		table,
		attributePairs.Assignments(),
		identifierPairs.Identifiers(),
	)
}

func (r *Record) DeleteSql(table string, primaryKeys []string) string {
	identifierPairs := NewAttributePairs()
	for _, k := range primaryKeys {
		identifierPairs.Add(Attribute(k), r.Value(k))
	}

	return fmt.Sprintf(
		"DELETE FROM `%s` WHERE %s",
		table,
		identifierPairs.Identifiers(),
	)
}

func (r *Record) Attributes() []string {
	return r.Columns
}

func (r *Record) Value(key string) Value {
	return NewValue(r.Data[key])
}

func (r *Record) Compare(other *Record, attributes []string) []string {
	diff := []string{}

	for _, k := range attributes {
		rValue := r.Value(k)
		otherValue := other.Value(k)

		if !rValue.Equals(otherValue) {
			diff = append(diff, k)
		}
	}

	return diff
}

func (r *Record) Equals(other *Record, attributes []string) bool {
	return len(r.Compare(other, attributes)) == 0
}

func (r *Record) CompareAll(other *Record) []string {
	if len(r.Data) != len(other.Data) {
		panic("record attributes differ in length")
	}
	for k := range r.Data {
		if _, ok := other.Data[k]; !ok {
			panic("incompatible record attributes")
		}
	}
	for k := range other.Data {
		if _, ok := r.Data[k]; !ok {
			panic("incompatible record attributes")
		}
	}

	return r.Compare(other, r.Attributes())
}

func (r *Record) EqualsAll(other *Record) bool {
	return len(r.CompareAll(other)) == 0
}

func (r *Record) IsBefore(other *Record, primaryKeys []string) bool {
	rValues := []Value{}
	otherValues := []Value{}
	for _, k := range primaryKeys {
		rValues = append(rValues, r.Value(k))
		otherValues = append(otherValues, other.Value(k))
	}

	isBefore := true
	for i := range primaryKeys {
		isBefore = isBefore && rValues[i].IsBefore(otherValues[i])
	}

	return isBefore
}

func (r *Record) Subset(primaryKeys, attributes []string) *Record {
	rSubset := NewRecord(append(primaryKeys, attributes...))
	for _, k := range rSubset.Columns {
		rSubset.Data[k] = r.Data[k]
	}
	return rSubset
}

func (r *Record) String() string {
	values := []string{}
	for _, k := range r.Attributes() {
		v := r.Value(k)
		values = append(values, v.String())
	}
	return fmt.Sprintf("(%s)", strings.Join(values, ", "))
}

type Attribute string

func (x Attribute) Key() string {
	return fmt.Sprintf("`%s`", x)
}

type AttributePairs struct {
	Keys   []string
	Values []Value
}

func NewAttributePairs() *AttributePairs {
	return &AttributePairs{
		Keys:   []string{},
		Values: []Value{},
	}
}

func (p *AttributePairs) Add(x Attribute, v Value) {
	p.Keys = append(p.Keys, x.Key())
	p.Values = append(p.Values, v)
}

func (p *AttributePairs) InsertColumns() string {
	return strings.Join(p.Keys, ", ")
}

func (p *AttributePairs) InsertValues() string {
	valuesStr := []string{}
	for _, v := range p.Values {
		valuesStr = append(valuesStr, v.String())
	}
	return strings.Join(valuesStr, ", ")
}

func (p *AttributePairs) Assignments() string {
	assignments := []string{}
	for i := range p.Keys {
		assignments = append(assignments, fmt.Sprintf(
			"%s = %s",
			p.Keys[i],
			p.Values[i].String(),
		))
	}
	return strings.Join(assignments, ", ")
}

func (p *AttributePairs) Identifiers() string {
	identifiers := []string{}
	for i := range p.Keys {
		identifiers = append(identifiers, fmt.Sprintf(
			"%s = %s",
			p.Keys[i],
			p.Values[i].String(),
		))
	}
	return strings.Join(identifiers, " AND ")
}
