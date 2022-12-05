package support

import (
	"bytes"
	"fmt"
	yaml "gopkg.in/yaml.v3"
	"io"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
)

//go:generate go run ../main.go -out ../../../gen/ systab ../spec/systab.yaml

type ScriptDef struct {
	Name           string            `yaml:"name"`
	Query          string            `yaml:"query"`
	Branches       int               `yaml:"branches"`
	Commits        int               `yaml:"commits"`
	Dummy          map[string]string `yaml:"dummy"`
	DiffsPerCommit int               `yaml:"diffsPerCommit"`
}

type ScriptsDef struct {
	Scripts []ScriptDef `yaml:"scripts"`
}

func ParseTestsFile(path string) (ScriptsDef, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return ScriptsDef{}, err
	}
	dec := yaml.NewDecoder(bytes.NewReader(contents))
	dec.KnownFields(true)
	var res ScriptsDef
	err = dec.Decode(&res)
	return res, err
}

type GenDefs interface{}

var _ GenDefs = ([]ScriptDef)(nil)

type ScriptGen struct {
	define ScriptDef
	w      io.Writer
}

func (g *ScriptGen) Generate(d ScriptDef, w io.Writer) {
	g.define = d

	g.w = w

	g.genThreadInit(d)
	g.genEvent()
	g.genDone()
	g.genCleanup()
	if d.Dummy != nil {
		g.genPrepareDummy(d)
	} else {
		g.genPrepareSys(d)
	}
}

func (g *ScriptGen) genCleanup() {
	fmt.Fprintf(g.w, "function cleanup()\n")
	fmt.Fprintf(g.w, "  local drv = sysbench.sql.driver()\n")
	fmt.Fprintf(g.w, "  local con = drv:connect()\n")
	fmt.Fprintf(g.w, "  local drv = sysbench.sql.driver()\n")
	for _, v := range g.define.Dummy {
		fmt.Fprintf(g.w, "  con:query(\"DROP TABLE IF EXISTS %s\")\n", v)
	}
	fmt.Fprintf(g.w, "  con:query(\"DROP TABLE IF EXISTS xy\")\n")
	fmt.Fprintf(g.w, "end\n\n")
}

func (g *ScriptGen) genPrepareSys(define ScriptDef) {
	fmt.Fprintf(g.w, "function prepare()\n")
	fmt.Fprintf(g.w, "  sysbench.opt.threads = 1\n")
	fmt.Fprintf(g.w, "  local drv = sysbench.sql.driver()\n")
	fmt.Fprintf(g.w, "  local con = drv:connect()\n")

	for i := 0; i < define.Branches; i++ {
		fmt.Fprintf(g.w, "  con:query(\"call dolt_checkout('-b', 'branch_%d')\"", i)
		fmt.Fprintf(g.w, "  con:query(\"call dolt_tag('tag_%d', 'head')\"", i)
	}

	fmt.Fprintf(g.w, "  con:query(\"create table xy (x int primary key, y varchar(20));\")\n")

	//TODO: generate commit/diffs
	for i := 0; i < define.Commits; i++ {
		fmt.Fprintf(g.w, "  con:query([[\n")
		fmt.Fprintf(g.w, "insert into xy values\n")
		for j := 0; j < define.DiffsPerCommit; j++ {
			// todo generate row for schema
			id := i*define.DiffsPerCommit + j
			fmt.Fprintf(g.w, "    (%d, 'row %d')", id, id)
			if j == define.DiffsPerCommit-1 {
				fmt.Fprintf(g.w, ";\n")
			} else {
				fmt.Fprintf(g.w, ",\n")
			}
		}
		fmt.Fprintf(g.w, "  ]])\n")
		fmt.Fprintf(g.w, "  con:query(\"call dolt_add('.');\")\n")
		fmt.Fprintf(g.w, "  con:query(\"call dolt_commit('-m', 'commit %d');\")\n", i)
	}
	fmt.Fprintf(g.w, "end\n\n")

}

//
//func (g *ScriptGen) dummyInserter(table string, ins inserter) {
//	lastCm := commit{hash: "NULL", time: "NULL", toX: "NULL", toY: "NULL"}
//	var thisCm commit
//	for i := 0; i < g.define.Commits; i++ {
//		if !ins.perCommit || (ins.perCommit && i == 0) {
//			fmt.Fprintf(g.w, "  con:query([[\n")
//			fmt.Fprintf(g.w, "insert into %s values\n", table)
//		}
//		end := g.define.DiffsPerCommit
//		if ins.isHistory {
//			end *= i + 1
//		}
//		thisCm = randCommit()
//		for j := 0; j < end; j++ {
//			id := i*g.define.DiffsPerCommit + j
//			if ins.isHistory {
//				id = j
//			}
//			thisCm.toX = fmt.Sprintf("%d", id)
//			thisCm.toY = fmt.Sprintf("row %d", id)
//			values := ins.vals(thisCm, lastCm)
//			if values == nil {
//				lastCm = thisCm
//				continue
//			}
//			fmt.Fprintf(g.w, ins.format(lastCm), values...)
//			lastCm = thisCm
//			if ins.perCommit {
//				if i == g.define.Commits-1 {
//					fmt.Fprintf(g.w, ";\n")
//				} else {
//					fmt.Fprintf(g.w, ",\n")
//				}
//				break
//			} else {
//				if j == end-1 {
//					fmt.Fprintf(g.w, ";\n")
//				} else {
//					fmt.Fprintf(g.w, ",\n")
//				}
//			}
//		}
//		if !ins.perCommit {
//			fmt.Fprintf(g.w, "]])\n")
//		}
//	}
//	if ins.perCommit {
//		fmt.Fprintf(g.w, "]])\n")
//	}
//}

type tableInserter struct {
	schema    []byte
	format    func(lastCm commit) string
	vals      func(thisCm, lastCm commit) []interface{}
	perCommit bool
	isHistory bool
	table     string
}

func newInserter(w io.Writer, c, d int) *inserter {
	return &inserter{
		w: w,
		c: c,
		d: d,
	}
}

type inserter struct {
	cbs []tableInserter
	w   io.Writer
	c   int
	d   int
	i   int
}

func (ins *inserter) append(i tableInserter) {
	ins.cbs = append(ins.cbs, i)
}

func (ins *inserter) generate() {
	lastCm := commit{hash: "NULL", time: "NULL", toX: "NULL", toY: "NULL"}
	var thisCm commit
	bufs := make([]bytes.Buffer, len(ins.cbs))

	for ins.i < ins.c {
		thisCm = randCommit()
		for i, cb := range ins.cbs {
			switch {
			case cb.isHistory:
				ins.insertHistory(&bufs[i], cb, thisCm, lastCm)
			case cb.perCommit:
				ins.insertPerCommit(&bufs[i], cb, thisCm, lastCm)
			default:
				ins.insertPerDiff(&bufs[i], cb, thisCm, lastCm)
			}
		}
		lastCm = thisCm
		ins.i++
	}

	for i := range bufs {
		ins.w.Write(ins.cbs[i].schema)
		ins.w.Write(bufs[i].Bytes())
	}
}

func (ins *inserter) insertPerCommit(buf *bytes.Buffer, cb tableInserter, thisCm, lastCm commit) {
	if ins.i == 0 {
		fmt.Fprintf(buf, "  con:query([[\n")
		fmt.Fprintf(buf, "insert into %s values\n", cb.table)
	}

	values := cb.vals(thisCm, lastCm)
	if values == nil {
		return
	}

	fmt.Fprintf(buf, cb.format(lastCm), values...)

	if ins.i == ins.c-1 {
		fmt.Fprintf(buf, ";\n")
		fmt.Fprintf(buf, "]])\n")
	} else {
		fmt.Fprintf(buf, ",\n")
	}
}

func (ins *inserter) insertPerDiff(buf *bytes.Buffer, cb tableInserter, thisCm, lastCm commit) {
	fmt.Fprintf(buf, "  con:query([[\n")
	fmt.Fprintf(buf, "insert into %s values\n", cb.table)

	for j := ins.i * ins.d; j < ins.d*(ins.i+1); j++ {
		if lastCm.toX == "" {
			lastCm.toX = fmt.Sprintf("%d", j-1)
			lastCm.toY = fmt.Sprintf("row %d", j-1)
		}
		thisCm.toX = fmt.Sprintf("%d", j)
		thisCm.toY = fmt.Sprintf("row %d", j)
		fmt.Fprintf(buf, cb.format(lastCm), cb.vals(thisCm, lastCm)...)
		lastCm.toX = thisCm.toX
		lastCm.toY = thisCm.toY
		if j == ins.d*(ins.i+1)-1 {
			fmt.Fprintf(buf, ";\n")
			fmt.Fprintf(buf, "]])\n")
		} else {
			fmt.Fprintf(buf, ",\n")
		}
	}
}

func (ins *inserter) insertHistory(buf *bytes.Buffer, cb tableInserter, thisCm, lastCm commit) {
	fmt.Fprintf(buf, "  con:query([[\n")
	fmt.Fprintf(buf, "insert into %s values\n", cb.table)

	for j := 0; j < ins.d*(ins.i+1); j++ {
		thisCm.toX = fmt.Sprintf("%d", j)
		thisCm.toY = fmt.Sprintf("row %d", j)
		fmt.Fprintf(buf, cb.format(lastCm), cb.vals(thisCm, lastCm)...)
		lastCm.toX = thisCm.toX
		lastCm.toY = thisCm.toY
		if j == ins.d*(ins.i+1)-1 {
			fmt.Fprintf(buf, ";\n")
			fmt.Fprintf(buf, "]])\n")
		} else {
			fmt.Fprintf(buf, ",\n")
		}
	}
}

func (g *ScriptGen) genPrepareDummy(define ScriptDef) {
	// dummy will insert rows that mimic the branch, tag, commit, and diff numbers
	fmt.Fprintf(g.w, "function prepare()\n")
	fmt.Fprintf(g.w, "  sysbench.opt.threads = 1\n")
	fmt.Fprintf(g.w, "  local drv = sysbench.sql.driver()\n")
	fmt.Fprintf(g.w, "  local con = drv:connect()\n")

	inserters := newInserter(g.w, define.Commits, define.DiffsPerCommit)

	for k, v := range define.Dummy {
		switch k {
		case "dolt_diff_xy", "dolt_commit_diff_xy":
			sch := bytes.Buffer{}
			fmt.Fprintf(&sch, "  con:query([[\n")
			fmt.Fprintf(&sch, "create table %s (\n", v)
			fmt.Fprintf(&sch, "  from_commit varchar(32),\n")
			fmt.Fprintf(&sch, "  from_commit_date datetime,\n")
			fmt.Fprintf(&sch, "  to_commit varchar(32),\n")
			fmt.Fprintf(&sch, "  to_commit_date datetime,\n")
			fmt.Fprintf(&sch, "  diff_type varchar(20),\n")
			fmt.Fprintf(&sch, "  to_x int,\n")
			fmt.Fprintf(&sch, "  to_y varchar(20),\n")
			fmt.Fprintf(&sch, "  from_x int,\n")
			fmt.Fprintf(&sch, "  from_y varchar(20),\n")
			fmt.Fprintf(&sch, "  index (from_commit),\n")
			fmt.Fprintf(&sch, "  index (to_commit)\n")
			fmt.Fprintf(&sch, "  )\n")
			fmt.Fprintf(&sch, "]])\n")
			inserters.append(tableInserter{
				table:     v,
				schema:    sch.Bytes(),
				perCommit: false,
				isHistory: false,
				vals: func(thisCm, lastCm commit) []interface{} {
					return []interface{}{lastCm.hash, lastCm.time, thisCm.hash, thisCm.time, "added", thisCm.toX, thisCm.toY, lastCm.toX, lastCm.toY}
				},
				format: func(lastCm commit) string {
					if lastCm.hash == "NULL" {
						if lastCm.toX != "NULL" {
							return "  (%s, %s, '%s', '%s', '%s', %s, '%s', %s, '%s')"
						}
						return "  (%s, %s, '%s', '%s', '%s', %s, '%s', %s, %s)"
					}
					return "  ('%s', '%s', '%s', '%s', '%s', %s, '%s', %s, '%s')"
				},
			})
		case "dolt_history_xy":
			sch := bytes.Buffer{}
			fmt.Fprintf(&sch, "  con:query([[\n")
			fmt.Fprintf(&sch, "create table %s (\n", v)
			fmt.Fprintf(&sch, "  commit_hash varchar(32),\n")
			fmt.Fprintf(&sch, "  committer varchar(20),\n")
			fmt.Fprintf(&sch, "  commit_date datetime,\n")
			fmt.Fprintf(&sch, "  x int,\n")
			fmt.Fprintf(&sch, "  y varchar(20),\n")
			fmt.Fprintf(&sch, "  primary key (commit_hash, x, y),\n")
			fmt.Fprintf(&sch, "  index (commit_hash)\n")
			fmt.Fprintf(&sch, ")\n")
			fmt.Fprintf(&sch, "  ]])\n")
			inserters.append(tableInserter{
				table:     v,
				schema:    sch.Bytes(),
				perCommit: false,
				isHistory: true,
				vals: func(thisCm, _ commit) []interface{} {
					return []interface{}{thisCm.hash, thisCm.committer, thisCm.time, thisCm.toX, thisCm.toY}
				},
				format: func(_ commit) string {
					return "  ('%s', '%s', '%s', %s, '%s')"
				},
			})
		case "dolt_log", "dolt_commits":
			sch := bytes.Buffer{}
			fmt.Fprintf(&sch, "  con:query([[\n")
			fmt.Fprintf(&sch, "create table %s (\n", v)
			fmt.Fprintf(&sch, "  commit_hash varchar(32),\n")
			fmt.Fprintf(&sch, "  committer varchar(20),\n")
			fmt.Fprintf(&sch, "  date datetime,\n")
			fmt.Fprintf(&sch, "  email varchar(100),\n")
			fmt.Fprintf(&sch, "  message varchar(100),\n")
			fmt.Fprintf(&sch, "  primary key (commit_hash)\n")
			fmt.Fprintf(&sch, ")\n")
			fmt.Fprintf(&sch, "  ]])\n")
			inserters.append(tableInserter{
				table:     v,
				schema:    sch.Bytes(),
				perCommit: true,
				isHistory: false,
				vals: func(thisCm, _ commit) []interface{} {
					return []interface{}{thisCm.hash, thisCm.committer, thisCm.time, thisCm.email, thisCm.message}
				},
				format: func(_ commit) string {
					return "  ('%s', '%s', '%s', '%s', '%s')"
				},
			})
		case "dolt_diff":
			sch := bytes.Buffer{}
			fmt.Fprintf(&sch, "  con:query([[\n")
			fmt.Fprintf(&sch, "create table %s (\n", v)
			fmt.Fprintf(&sch, "  commit_hash varchar(32),\n")
			fmt.Fprintf(&sch, "  table_name varchar(20),\n")
			fmt.Fprintf(&sch, "  committer varchar(20),\n")
			fmt.Fprintf(&sch, "  date datetime,\n")
			fmt.Fprintf(&sch, "  email varchar(100),\n")
			fmt.Fprintf(&sch, "  message varchar(100),\n")
			fmt.Fprintf(&sch, "  primary key (commit_hash, table_name),\n")
			fmt.Fprintf(&sch, "  index (commit_hash)\n")
			fmt.Fprintf(&sch, ")\n")
			fmt.Fprintf(&sch, "  ]])\n")
			inserters.append(tableInserter{
				table:     v,
				schema:    sch.Bytes(),
				perCommit: true,
				isHistory: false,
				vals: func(thisCm, _ commit) []interface{} {
					return []interface{}{thisCm.hash, "xy", thisCm.committer, thisCm.time, thisCm.email, thisCm.message}
				},
				format: func(_ commit) string {
					return "  ('%s', '%s', '%s', '%s', '%s', '%s')"
				},
			})
		case "dolt_branches":
			fmt.Fprintf(g.w, "  con:query([[\n")
			fmt.Fprintf(g.w, "create table %s (\n", v)
			fmt.Fprintf(g.w, "  name varchar(32),\n")
			fmt.Fprintf(g.w, "  hash varchar(20),\n")
			fmt.Fprintf(g.w, "  latest_committer varchar(20),\n")
			fmt.Fprintf(g.w, "  latest_committer_email datetime,\n")
			fmt.Fprintf(g.w, "  latest_commit_date varchar(100),\n")
			fmt.Fprintf(g.w, "  latest_commit_message varchar(100),\n")
			fmt.Fprintf(g.w, "  primary key (name),\n")
			fmt.Fprintf(g.w, "  index (hash)\n")
			fmt.Fprintf(g.w, ")\n")
			fmt.Fprintf(g.w, "  ]])\n")
			inserters.append(tableInserter{
				table:     v,
				perCommit: true,
				isHistory: false,
				vals: func(thisCm, _ commit) []interface{} {
					id, err := strconv.Atoi(thisCm.toX)
					if err != nil {
						panic(err)
					}
					return []interface{}{fmt.Sprintf("branch_%d", id), thisCm.hash, thisCm.committer, thisCm.email, thisCm.time, thisCm.message}
				},
				format: func(_ commit) string {
					return "  ('%s', '%s', '%s', '%s', '%s', '%s')"
				},
			})
		case "dolt_tags":
			fmt.Fprintf(g.w, "  con:query([[\n")
			fmt.Fprintf(g.w, "create table %s (\n", v)
			fmt.Fprintf(g.w, "  tag_name varchar(32),\n")
			fmt.Fprintf(g.w, "  tag_hash varchar(20),\n")
			fmt.Fprintf(g.w, "  tagger varchar(20),\n")
			fmt.Fprintf(g.w, "  date datetime,\n")
			fmt.Fprintf(g.w, "  email varchar(100),\n")
			fmt.Fprintf(g.w, "  message varchar(100),\n")
			fmt.Fprintf(g.w, "  primary key (tag_name, tag_hash),\n")
			fmt.Fprintf(g.w, "  index (tag_hash)\n")
			fmt.Fprintf(g.w, ")\n")
			fmt.Fprintf(g.w, "  ]])\n")
			inserters.append(tableInserter{
				table:     v,
				perCommit: true,
				isHistory: false,
				vals: func(thisCm, _ commit) []interface{} {
					id, err := strconv.Atoi(thisCm.toX)
					if err != nil {
						panic(err)
					}
					return []interface{}{fmt.Sprintf("tag_%d", id), thisCm.hash, thisCm.committer, thisCm.email, thisCm.time, thisCm.message}
				},
				format: func(_ commit) string {
					return "  ('%s', '%s', '%s', '%s', '%s', '%s')"
				},
			})
		case "dolt_commit_ancestors":
			fmt.Fprintf(g.w, "  con:query([[\n")
			fmt.Fprintf(g.w, "create table %s (\n", v)
			fmt.Fprintf(g.w, "  commit_hash varchar(32),\n")
			fmt.Fprintf(g.w, "  parent_hash varchar(32),\n")
			fmt.Fprintf(g.w, "  parent_index int,\n")
			fmt.Fprintf(g.w, "  primary key (commit_hash, parent_hash, parent_index),\n")
			fmt.Fprintf(g.w, "  index (commit_hash),\n")
			fmt.Fprintf(g.w, "  index (parent_hash)\n")
			fmt.Fprintf(g.w, ")\n")
			fmt.Fprintf(g.w, "  ]])\n")
			inserters.append(tableInserter{
				table:     v,
				perCommit: true,
				isHistory: false,
				vals: func(thisCm, lastCm commit) []interface{} {
					if lastCm.hash == "NULL" {
						return nil
					}
					return []interface{}{thisCm.hash, lastCm.hash, "0"}
				},
				format: func(lastCm commit) string {
					return "  ('%s', '%s', %s)"
				},
			})
		default:
			log.Fatalf("unknown system table: '%s'", k)
		}
	}

	inserters.generate()

	fmt.Fprintf(g.w, "end\n\n")
}

func (g *ScriptGen) genThreadInit(define ScriptDef) {
	fmt.Fprintf(g.w, "function thread_init()\n")
	fmt.Fprintf(g.w, "  drv = sysbench.sql.driver()\n")
	fmt.Fprintf(g.w, "  con = drv:connect()\n")
	fmt.Fprintf(g.w, "  stmt = con:prepare('%s')\n", define.Query)
	fmt.Fprintf(g.w, "end\n\n")
}

func (g *ScriptGen) genDone() {
	fmt.Fprintf(g.w, "function thread_done()\n")
	fmt.Fprintf(g.w, "  stmt:close()\n")
	fmt.Fprintf(g.w, "  con:disconnect()\n")
	fmt.Fprintf(g.w, "end\n\n")
}

func (g *ScriptGen) genEvent() {
	fmt.Fprintf(g.w, "function event()\n")
	fmt.Fprintf(g.w, "  stmt:execute()\n")
	fmt.Fprintf(g.w, "end\n\n")
}

const letterBytes = "abcdefghijklmnopqrstuvwxyz123456789"
const hashLen = 32

func randHash() string {
	b := make([]byte, hashLen)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

type commit struct {
	hash      string
	time      string
	toX       string
	toY       string
	committer string
	email     string
	message   string
}

func randCommit() commit {
	return commit{
		hash:      randHash(),
		time:      time.Now().Format("2006-01-02T15:04:05Z07:00"),
		email:     "max@dolthub.com",
		committer: "Max Hoffman",
		message:   "a commit message",
	}
}
