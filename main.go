package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/michaelrk02/sqldiff/internal"
)

type Options struct {
	Left     string
	Right    string
	Table    string
	Keys     string
	Strategy string
	Patch    string
}

func (o Options) Validate() bool {
	return o.Left != "" &&
		o.Right != "" &&
		o.Table != "" &&
		o.Keys != "" &&
		internal.In(o.Strategy, []string{"keys", "all"})
}

func main() {
	var opt Options

	flag.StringVar(&opt.Left, "left", "", "left connection name")
	flag.StringVar(&opt.Right, "right", "", "right connection name")
	flag.StringVar(&opt.Table, "table", "", "table to compare")
	flag.StringVar(&opt.Keys, "keys", "", "primary keys (comma-separated)")
	flag.StringVar(&opt.Strategy, "strategy", "keys", "compare strategy (keys/all)")
	flag.StringVar(&opt.Patch, "patch", "", "patch options: (i)nsert, (u)pdate, (d)elete")
	flag.Parse()

	if !opt.Validate() {
		flag.PrintDefaults()
		return
	}

	cfg, err := internal.LoadConfig()
	if err != nil {
		panic(err)
	}

	leftConnProps, ok := cfg.Connections[opt.Left]
	if !ok {
		panic(fmt.Sprintf("invalid connection name: %s", opt.Left))
	}

	rightConnProps, ok := cfg.Connections[opt.Right]
	if !ok {
		panic(fmt.Sprintf("invalid connection name: %s", opt.Right))
	}

	left, err := internal.NewConnection(opt.Left, leftConnProps)
	if err != nil {
		panic(err)
	}

	right, err := internal.NewConnection(opt.Right, rightConnProps)
	if err != nil {
		panic(err)
	}

	primaryKeys := strings.Split(opt.Keys, ",")

	patchOptions := internal.PatchOption(0)
	for _, p := range opt.Patch {
		if p == 'i' {
			patchOptions |= internal.PatchOptionInsert
		}
		if p == 'u' {
			patchOptions |= internal.PatchOptionUpdate
		}
		if p == 'd' {
			patchOptions |= internal.PatchOptionDelete
		}
	}

	diff := internal.NewDiff(
		left,
		right,
		opt.Table,
		primaryKeys,
		internal.CompareStrategy(opt.Strategy),
		os.Stdout,
	)

	patch, err := diff.Compare(patchOptions)
	if err != nil {
		panic(err)
	}

	if opt.Patch != "" {
		func() {
			f, err := os.Create(fmt.Sprintf("%s.%s.patch.sql", opt.Left, opt.Right))
			if err != nil {
				panic(err)
			}
			defer f.Close()

			patch.Write(f)
		}()
	}
}
