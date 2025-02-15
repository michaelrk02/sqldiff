package internal

import (
	"fmt"
	"io"
	"strings"
)

type CompareStrategy string

const (
	CompareStrategyKeys CompareStrategy = "keys"
	CompareStrategyAll  CompareStrategy = "all"
)

type Diff struct {
	left        *Connection
	right       *Connection
	table       string
	primaryKeys []string
	strategy    CompareStrategy
	output      io.Writer
}

func NewDiff(left, right *Connection, table string, primaryKeys []string, strategy CompareStrategy, output io.Writer) *Diff {
	return &Diff{
		left:        left,
		right:       right,
		table:       table,
		primaryKeys: primaryKeys,
		strategy:    strategy,
		output:      output,
	}
}

func (d *Diff) Compare(patchOptions PatchOption) (*Patch, error) {
	p := NewPatch(d.table, d.primaryKeys, patchOptions)

	fmt.Fprintf(d.output, "--- %s\n", d.left.Name)
	fmt.Fprintf(d.output, "+++ %s\n", d.right.Name)
	fmt.Fprintln(d.output)

	leftRows, err := d.left.Queryx(fmt.Sprintf("SELECT * FROM `%s` %s", d.table, d.orderBy()))
	if err != nil {
		return nil, err
	}
	defer leftRows.Close()

	rightRows, err := d.right.Queryx(fmt.Sprintf("SELECT * FROM `%s` %s", d.table, d.orderBy()))
	if err != nil {
		return nil, err
	}
	defer rightRows.Close()

	forwardLeft := true
	forwardRight := true

	leftOk := true
	rightOk := true

	var leftRecord, rightRecord *Record
	for {
		if !leftOk {
			forwardLeft = false
		}
		if !rightOk {
			forwardRight = false
		}
		if !forwardLeft && !forwardRight {
			break
		}

		if forwardLeft {
			leftOk = leftRows.Next()
			if leftOk {
				cols, err := leftRows.Columns()
				if err != nil {
					return nil, err
				}

				firstRecord := leftRecord == nil

				leftRecord = NewRecord(cols)
				err = leftRows.MapScan(leftRecord.Data)
				if err != nil {
					return nil, err
				}
				forwardLeft = false

				if firstRecord {
					fmt.Fprintf(d.output, "=== (%s)\n", strings.Join(leftRecord.Attributes(), ", "))
				}
			}
		}

		if forwardRight {
			rightOk = rightRows.Next()
			if rightOk {
				cols, err := rightRows.Columns()
				if err != nil {
					return nil, err
				}

				rightRecord = NewRecord(cols)
				err = rightRows.MapScan(rightRecord.Data)
				if err != nil {
					return nil, err
				}
				forwardRight = false
			}
		}

		if d.strategy == CompareStrategyKeys {
			if leftRecord.Equals(rightRecord, d.primaryKeys) {
				forwardLeft = true
				forwardRight = true
			} else {
				if rightRecord.IsBefore(leftRecord, d.primaryKeys) {
					fmt.Fprintf(d.output, "+++ %s\n", rightRecord.String())
					p.ToInsert = append(p.ToInsert, rightRecord)
					forwardRight = true
				} else if leftRecord.IsBefore(rightRecord, d.primaryKeys) {
					fmt.Fprintf(d.output, "--- %s\n", leftRecord.String())
					p.ToDelete = append(p.ToDelete, leftRecord)
					forwardLeft = true
				}
			}
		} else if d.strategy == CompareStrategyAll {
			cmp := leftRecord.CompareAll(rightRecord)
			if len(cmp) == 0 {
				forwardLeft = true
				forwardRight = true
			} else {
				if !leftRecord.Equals(rightRecord, d.primaryKeys) {
					if rightRecord.IsBefore(leftRecord, d.primaryKeys) {
						fmt.Fprintf(d.output, "+++ %s\n", rightRecord.String())
						p.ToInsert = append(p.ToInsert, rightRecord)
						forwardRight = true
					} else if leftRecord.IsBefore(rightRecord, d.primaryKeys) {
						fmt.Fprintf(d.output, "--- %s\n", leftRecord.String())
						p.ToDelete = append(p.ToDelete, leftRecord)
						forwardLeft = true
					}
				} else {
					fmt.Fprintf(d.output, ">>> %s\n", rightRecord.String())
					u := rightRecord.Subset(d.primaryKeys, cmp)
					p.ToUpdate = append(p.ToUpdate, u)
					forwardLeft = true
					forwardRight = true
				}
			}
		}
	}

	return p, nil
}

func (d *Diff) orderBy() string {
	orderBy := []string{}
	for _, k := range d.primaryKeys {
		orderBy = append(orderBy, fmt.Sprintf("`%s` ASC", k))
	}
	return fmt.Sprintf(
		"ORDER BY %s",
		strings.Join(orderBy, ", "),
	)
}
