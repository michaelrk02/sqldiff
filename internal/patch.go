package internal

import (
	"fmt"
	"io"
)

type PatchOption int

func (x PatchOption) Has(opt PatchOption) bool {
	return x&opt != 0
}

const (
	PatchOptionInsert PatchOption = 1 << iota
	PatchOptionUpdate
	PatchOptionDelete
)

type Patch struct {
	table       string
	primaryKeys []string
	options     PatchOption

	ToInsert []*Record
	ToUpdate []*Record
	ToDelete []*Record
}

func NewPatch(table string, primaryKeys []string, options PatchOption) *Patch {
	return &Patch{
		table:       table,
		primaryKeys: primaryKeys,
		options:     options,
		ToInsert:    []*Record{},
		ToUpdate:    []*Record{},
		ToDelete:    []*Record{},
	}
}

func (p *Patch) Write(w io.Writer) {
	if p.options.Has(PatchOptionInsert) {
		for _, r := range p.ToInsert {
			fmt.Fprintf(w, "%s;\n", r.InsertSql(p.table))
		}
		fmt.Fprintln(w)
	}

	if p.options.Has(PatchOptionUpdate) {
		for _, r := range p.ToUpdate {
			fmt.Fprintf(w, "%s;\n", r.UpdateSql(p.table, p.primaryKeys, Filter(
				r.Attributes(),
				func(v string) bool {
					if In(v, p.primaryKeys) {
						return false
					}
					return true
				},
			)))
		}
		fmt.Fprintln(w)
	}

	if p.options.Has(PatchOptionDelete) {
		for _, r := range p.ToDelete {
			fmt.Fprintf(w, "%s -- %s;\n", r.DeleteSql(p.table, p.primaryKeys), r.String())
		}
		fmt.Fprintln(w)
	}
}
